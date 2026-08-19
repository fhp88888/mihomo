package route

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/metacubex/mihomo/adapter/outboundgroup"
	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/component/smart"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	"github.com/metacubex/mihomo/tunnel"

	"github.com/metacubex/chi"
	"github.com/metacubex/chi/render"
	"github.com/metacubex/http"
)

func groupRouter() http.Handler {
	r := chi.NewRouter()
	r.Get("/", getGroups)
	r.Get("/weights", getAllGroupWeights)

	r.Route("/{name}", func(r chi.Router) {
		r.Use(parseProxyName, findProxyByName)
		r.Get("/", getGroup)
		r.Get("/delay", getGroupDelay)
		r.Get("/weights", getGroupWeights)
	})
	return r
}

func getGroups(w http.ResponseWriter, r *http.Request) {
	var gs []C.Proxy
	for _, p := range tunnel.Proxies() {
		if _, ok := p.Adapter().(outboundgroup.ProxyGroup); ok {
			gs = append(gs, p)
		}
	}
	render.JSON(w, r, render.M{
		"proxies": gs,
	})
}

func getGroup(w http.ResponseWriter, r *http.Request) {
	proxy := r.Context().Value(CtxKeyProxy).(C.Proxy)
	if _, ok := proxy.Adapter().(outboundgroup.ProxyGroup); ok {
		render.JSON(w, r, proxy)
		return
	}
	render.Status(r, http.StatusNotFound)
	render.JSON(w, r, ErrNotFound)
}

func getGroupDelay(w http.ResponseWriter, r *http.Request) {
	proxy := r.Context().Value(CtxKeyProxy).(C.Proxy)
	group, ok := proxy.Adapter().(outboundgroup.ProxyGroup)
	if !ok {
		render.Status(r, http.StatusNotFound)
		render.JSON(w, r, ErrNotFound)
		return
	}

	if selectAble, ok := proxy.Adapter().(outboundgroup.SelectAble); ok && proxy.Type() != C.Selector {
		selectAble.ForceSet("")
		cachefile.Cache().SetSelected(proxy.Name(), "")
	}

	query := r.URL.Query()
	url := query.Get("url")
	timeout, err := strconv.ParseInt(query.Get("timeout"), 10, 32)
	if err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}

	expectedStatus, err := utils.NewUnsignedRanges[uint16](query.Get("expected"))
	if err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, ErrBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Millisecond*time.Duration(timeout))
	defer cancel()

	dm, err := group.URLTest(ctx, url, expectedStatus)
	if err != nil {
		render.Status(r, http.StatusGatewayTimeout)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	render.JSON(w, r, dm)
}

func getGroupWeights(w http.ResponseWriter, r *http.Request) {
	proxy := r.Context().Value(CtxKeyProxy).(C.Proxy)
	smartGroup, ok := proxy.Adapter().(*outboundgroup.Smart)
	if !ok {
		log.Debugln("[Smart] Failed to request weight ranking: Not a Smart group (actual type: %T)", proxy.Adapter())
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, render.M{
			"weights": []smart.NodeRankItem{},
			"error":   "Not a Smart group",
		})
		return
	}

	ranking := smartGroup.NodeRankingSnapshot()
	if len(ranking.Result) == 0 {
		log.Debugln("Policy group %s has no usage data", smartGroup.Name())
		render.JSON(w, r, render.M{
			"weights": []smart.NodeRankItem{},
			"message": "No usage data available for the specified group",
		})
		return
	}

	render.JSON(w, r, render.M{
		"weights": ranking.Result,
	})
}

func getAllGroupWeights(w http.ResponseWriter, r *http.Request) {
	result := make(map[string][]smart.NodeRankItem)
	errorsMap := make(map[string]string)

	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, 5)
	)

	for _, p := range tunnel.Proxies() {
		sg, ok := p.Adapter().(*outboundgroup.Smart)
		if !ok {
			continue
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(sg *outboundgroup.Smart) {
			defer wg.Done()
			defer func() { <-sem }()

			ranking := sg.NodeRankingSnapshot()
			mu.Lock()
			defer mu.Unlock()
			result[sg.Name()] = ranking.Result
		}(sg)
	}

	wg.Wait()

	if len(result) == 0 && len(errorsMap) == 0 {
		render.JSON(w, r, render.M{
			"weights": map[string][]smart.NodeRankItem{},
			"message": "No Smart groups or no weight data available",
		})
		return
	}

	render.JSON(w, r, render.M{
		"weights": result,
		"errors":  errorsMap,
	})
}