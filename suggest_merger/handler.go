package merger

import (
  "context"
  "encoding/json"
  "fmt"
  "golang.org/x/sync/errgroup"
  "io/ioutil"
  "log"
  "main/suggest"
  "net/http"
  "net/url"
  "time"
)

type Handler struct {
  Config        *Config
  SuggestClient *SuggestClient
}

type SuggestClient struct {
  httpClient *http.Client
}

func NewSuggestClient() *SuggestClient {
  return &SuggestClient{
    httpClient: &http.Client{
      Timeout: time.Second * 10,
    },
  }
}

func get(requestURL string, headers http.Header, client *http.Client) (int, []byte, http.Header, error) {
  req, err := http.NewRequest("GET", requestURL, nil)
  if err != nil {
    return 0, nil, nil, fmt.Errorf("cannot create request for the url %s: %v", requestURL, err)
  }
  req.Header = headers
  res, err := client.Do(req)
  if err != nil {
    return 0, nil, nil, fmt.Errorf("cannot execute request for the url %s: %v", requestURL, err)
  }
  content, err := ioutil.ReadAll(res.Body)
  return res.StatusCode, content, res.Header, err
}

func (h *Handler) HandleMergerSuggestRequest(w http.ResponseWriter, r *http.Request) {
  part := r.URL.Query().Get("part")

  doRequests := func(ctx context.Context, query string) ([]*suggest.PaginatedSuggestResponse, error) {
    g, ctx := errgroup.WithContext(ctx)

    results := make([]*suggest.PaginatedSuggestResponse, len(h.Config.SuggestShardsUrls))
    for i, suggestShardUrl := range h.Config.SuggestShardsUrls {
      i, suggestShardUrl := i, suggestShardUrl // https://golang.org/doc/faq#closures_and_goroutines

      newSuggestShardUrl, err := url.Parse(suggestShardUrl)
      if err != nil {
        log.Fatal(err)
      }
      values := newSuggestShardUrl.Query()
      values.Add("part", query)
      values.Add("with-version", "true")
      newSuggestShardUrl.RawQuery = values.Encode()

      g.Go(func() error {
        _, result, _, err := get(newSuggestShardUrl.String(), r.Header, h.SuggestClient.httpClient)
        if err == nil {
          resp := &suggest.PaginatedSuggestResponse{}
          err := json.Unmarshal(result, resp)
          if err != nil {
            return err
          }
          results[i] = resp
        }
        return err
      })
    }
    if err := g.Wait(); err != nil {
      return nil, err
    }
    return results, nil
  }

  results, err := doRequests(context.Background(), part)
  if err != nil {
    log.Println(err)
  }

  resp := make([]*suggest.SuggestAnswerItem, 0)
  var maxVersion uint64
  for _, result := range results {
    if len(result.Suggestions) > 0 && result.Version > maxVersion {
      resp = result.Suggestions
      maxVersion = result.Version
    }
  }
  suggest.ReportSuccessData(w, resp)
}

func (h *Handler) HandleMergerHealthRequest(w http.ResponseWriter, _ *http.Request) {
  suggest.ReportSuccessMessage(w, "OK")
}
