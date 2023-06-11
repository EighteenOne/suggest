package merger

import (
  "bufio"
  "fmt"
  "github.com/microcosm-cc/bluemonday"
  "google.golang.org/protobuf/proto"
  "io"
  "io/ioutil"
  "log"
  "main/suggest"
  "main/tools"
  "os"
  "sort"
  "strings"
)

type SymbolStat struct {
  Count      int
  StartIndex int
  EndIndex   int
}

func getSymbolStatByPrefixes(inputFilePath string) (map[string]*SymbolStat, error) {
  symbolsMapCounter := map[string]*SymbolStat{}

  file, err := os.Open(inputFilePath)
  if err != nil {
    return nil, err
  }
  defer file.Close()

  scanner := bufio.NewScanner(file)
  lineNumber := 0
  currentSymbol := ""
  currentStartPos, currentEndPos, currentCounter := 0, 0, 0

  for scanner.Scan() {
    line := strings.TrimSpace(scanner.Text())

    if len(line) == 0 {
      continue
    }

    firstLineSymbol := strings.ToLower(line[0:1])

    if firstLineSymbol != currentSymbol {
      if currentSymbol != "" {
        currentEndPos += currentCounter - 1
        symbolsMapCounter[currentSymbol] = &SymbolStat{
          Count:      currentCounter,
          StartIndex: currentStartPos,
          EndIndex:   currentEndPos,
        }
        currentStartPos = currentEndPos + 1
        currentEndPos = currentStartPos
        currentCounter = 0
      }
      currentSymbol = firstLineSymbol
    }

    currentEndPos += len(line)
    currentCounter += 1

    lineNumber++
    if lineNumber%100000 == 0 {
      log.Printf("read %d lines", lineNumber)
    }
  }
  // processing for last symbols
  if currentSymbol != "" {
    currentEndPos += currentCounter - 1
    symbolsMapCounter[currentSymbol] = &SymbolStat{
      Count:      currentCounter,
      StartIndex: currentStartPos,
      EndIndex:   currentEndPos,
    }
  }

  return symbolsMapCounter, nil
}

func getIndexOfMin(items []float64) int {
  min := items[0]
  minIdx := 0
  for i, item := range items {
    if item < min {
      min = item
      minIdx = i
    }
  }
  return minIdx
}

func getDistributionByParts(statMap map[string]*SymbolStat, countParts int) (map[int][]string, error) {
  keys := make([]string, 0, len(statMap))
  sumWeights := 0

  for key, value := range statMap {
    keys = append(keys, key)
    sumWeights += value.Count
  }
  sort.SliceStable(keys, func(i, j int) bool {
    return statMap[keys[i]].Count > statMap[keys[j]].Count
  })

  // the first estimate of the maximum part volume is
  // the total volume divided to all parts
  maxSize := float64(sumWeights / countParts)

  // prepare array containing the current weight of the parts
  weightsParts := make([]float64, countParts)
  parts := map[int][]string{}
  restWeightsSum := sumWeights

  for _, k := range keys {
    weight := statMap[k].Count

    // put next value in part with lowest weight sum
    b := getIndexOfMin(weightsParts)

    // calculate new weight of this part
    newWeightSum := weightsParts[b] + float64(weight)
    foundPart := false
    for !foundPart {
      if newWeightSum <= maxSize {
        parts[b] = append(parts[b], k)
        weightsParts[b] = newWeightSum
        restWeightsSum -= weight
        foundPart = true
      } else {
        // if not, increase the max volume by the sum of
        // the rest of the parts per part
        //vPartMax += sum(weightsParts[item:]) / float(countParts)
        if restWeightsSum/countParts <= 1 {
          maxSize += float64(restWeightsSum)
        } else {
          maxSize += float64(restWeightsSum / countParts)
        }
      }
    }
  }
  return parts, nil
}

func LoadItemsByPart(inputFilePath string, startIndex int, endIndex int, policy *bluemonday.Policy) ([]*suggest.Item, error) {
  file, err := os.Open(inputFilePath)
  if err != nil {
    return nil, err
  }
  var items []*suggest.Item

  _, err = file.Seek(int64(startIndex), io.SeekStart)
  if err != nil {
    log.Fatal(err)
  }
  scanner := bufio.NewScanner(file)

  currentLen := startIndex
  lineNumber := 0
  for scanner.Scan() {
    line := strings.TrimSpace(scanner.Text())
    if len(line) == 0 {
      continue
    }
    item, err := suggest.NewItem(line, policy)
    if err != nil {
      return nil, fmt.Errorf("error processing line #%d: %v", lineNumber, err)
    }
    items = append(items, item)
    lineNumber++
    if lineNumber%100000 == 0 {
      log.Printf("read %d lines", lineNumber)
    }

    currentLen += len(line) + 1
    if currentLen >= endIndex {
      break
    }

  }
  return items, nil
}

func DoBuildShardedSuggest(inputFilePath string, suggestDataPath string, maxItemsPerPrefix int, suffixFactor float64, disableNormalizedParts bool, countOutputFiles int) {
  statMap, err := getSymbolStatByPrefixes(inputFilePath)
  if err != nil {
    log.Fatalln(err)
  }

  parts, err := getDistributionByParts(statMap, countOutputFiles)
  if err != nil {
    log.Fatalln(err)
  }

  policy := tools.GetPolicy()
  for shardNumber, prefixes := range parts {
    var items []*suggest.Item

    for _, prefix := range prefixes {
      itemsPart, err := LoadItemsByPart(inputFilePath, statMap[prefix].StartIndex, statMap[prefix].EndIndex, policy)
      if err != nil {
        log.Fatalln(err)
      }
      items = append(items, itemsPart...)
    }

    suggestData, err := suggest.BuildSuggestData(items, maxItemsPerPrefix, float32(suffixFactor), disableNormalizedParts)
    if err != nil {
      log.Fatalln(err)
    }

    log.Printf("marshalling suggest as proto")
    b, err := proto.Marshal(suggestData)
    if err != nil {
      log.Fatalln(err)
    }

    suggestDataPathPart := strings.ReplaceAll(suggestDataPath, ".", fmt.Sprintf("_%d.", shardNumber))

    log.Printf("writing the resulting proto suggest data to %s with prefixes %v, items count %d, version %d", suggestDataPathPart, prefixes, len(items), suggestData.Version)
    if err := ioutil.WriteFile(suggestDataPathPart, b, 0644); err != nil {
      log.Fatalln(err)
    }
  }
  return
}
