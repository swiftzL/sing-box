package outbound

import (
	"context"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/interrupt"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	log2 "log"
	"math/rand"
	"net"
	"time"
)

type RandomSelector struct {
	myOutboundAdapter
	ctx                          context.Context
	tags                         []string
	defaultTag                   string
	outbounds                    map[string]adapter.Outbound
	outboundList                 []adapter.Outbound
	selected                     adapter.Outbound
	interruptGroup               *interrupt.Group
	interruptExternalConnections bool
	lastOutbound                 adapter.Outbound
	lastTime                     int64
	outboundExistInterval        int
}

func (r RandomSelector) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	out := r.randomOutbound()
	return out.DialContext(ctx, network, destination)
}

func (r *RandomSelector) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	out := r.outboundList[rand.Intn(len(r.outboundList))]
	return out.ListenPacket(ctx, destination)
}

func (r *RandomSelector) NewConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext) error {
	out := r.randomOutbound()
	log2.Println("随机到 tag :", out.Tag())
	return out.NewConnection(ctx, conn, metadata)
}

func (r *RandomSelector) NewPacketConnection(
	ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext,
) error {
	out := r.randomOutbound()
	return out.NewPacketConnection(ctx, conn, metadata)
}

func (r *RandomSelector) randomOutbound() adapter.Outbound {
	if time.Now().Unix()-r.lastTime < int64(r.outboundExistInterval) {
		return r.lastOutbound
	}
	out := r.outboundList[rand.Intn(len(r.outboundList))]
	r.lastTime = time.Now().Unix()
	r.lastOutbound = out
	return r.lastOutbound
}

func (r *RandomSelector) Start() error {
	log2.Println("开始初始化 随机选择器")
	for i, tag := range r.tags {
		detour, loaded := r.router.Outbound(tag)
		if !loaded {
			return E.New("outbound ", i, " not found: ", tag)
		}
		r.outbounds[tag] = detour
		r.outboundList = append(r.outboundList, detour)
	}
	log2.Println("随机选择器完成")
	return nil
}

func NewRandomSelector(
	ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string,
	options option.RandomSelectorOutboundOptions,
) (*RandomSelector, error) {
	outbound := &RandomSelector{
		myOutboundAdapter: myOutboundAdapter{
			protocol:     C.TypeSelector,
			router:       router,
			logger:       logger,
			tag:          tag,
			dependencies: options.Outbounds,
			network:      []string{N.NetworkTCP},
		},
		ctx:                   ctx,
		tags:                  options.Outbounds,
		outbounds:             make(map[string]adapter.Outbound),
		interruptGroup:        interrupt.NewGroup(),
		outboundExistInterval: 20, //
	}
	if len(outbound.tags) == 0 {
		return nil, E.New("missing tags")
	}
	return outbound, nil
}
