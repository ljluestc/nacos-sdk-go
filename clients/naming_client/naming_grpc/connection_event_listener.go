/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package naming_grpc

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client/naming_proxy"

	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/common/logger"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/util"
)

type ConnectionEventListener struct {
	clientProxy              naming_proxy.INamingProxy
	registeredInstanceCached sync.Map
	subscribes               sync.Map
}

func NewConnectionEventListener(clientProxy naming_proxy.INamingProxy) *ConnectionEventListener {
	return &ConnectionEventListener{
		clientProxy: clientProxy,
	}
}

func (c *ConnectionEventListener) OnConnected() {
	c.redoSubscribe()
	c.redoRegisterEachService()
}

func (c *ConnectionEventListener) OnDisConnect() {
	c.SubscriberOnDisconnect()
}

// SubscriberOnDisconnect resets the confirmed state of every cached subscriber
// redo entry, aligning with the Java SDK NamingGrpcRedoService onDisConnect:
// subscriptions confirmed on the lost connection become unconfirmed, so the
// reconnect redo retries them and IsSubscriberRegistered no longer reports
// them as subscribed.
func (c *ConnectionEventListener) SubscriberOnDisconnect() {
	c.subscribes.Range(func(_, entry interface{}) bool {
		if redoEntry, ok := entry.(*subscriberRedoEntry); ok {
			redoEntry.registered.Store(false)
		}
		return true
	})
}

func (c *ConnectionEventListener) redoSubscribe() {
	grpcProxy, ok := c.clientProxy.(*NamingGrpcProxy)
	if !ok {
		logger.Error("redo subscribe clientProxy type error")
		return
	}
	c.subscribes.Range(func(k, _ interface{}) bool {
		key := k.(string)
		info := strings.Split(key, constant.SERVICE_INFO_SPLITER)
		var err error
		var service model.Service
		if len(info) > 2 {
			service, err = c.clientProxy.Subscribe(info[1], info[0], info[2])
		} else {
			service, err = c.clientProxy.Subscribe(info[1], info[0], "")
		}
		if err != nil {
			logger.Warnf("redo subscribe service:%s faild:%+v", info[1], err)
			return true
		}
		grpcProxy.serviceInfoHolder.ProcessService(&service)
		return true
	})
}

func (c *ConnectionEventListener) redoRegisterEachService() {
	c.registeredInstanceCached.Range(func(k, v interface{}) bool {
		info := strings.Split(k.(string), constant.SERVICE_INFO_SPLITER)
		serviceName := info[1]
		groupName := info[0]
		if instance, ok := v.(model.Instance); ok {
			if _, err := c.clientProxy.RegisterInstance(serviceName, groupName, instance); err != nil {
				logger.Warnf("redo register service:%s groupName:%s faild:%s", info[1], info[0], err.Error())
				return true
			}
		}
		if instances, ok := v.([]model.Instance); ok {
			if _, err := c.clientProxy.BatchRegisterInstance(serviceName, groupName, instances); err != nil {
				logger.Warnf("redo batch register service:%s groupName:%s faild:%s", info[1], info[0], err.Error())
				return true
			}
		}
		return true
	})
}

func (c *ConnectionEventListener) CacheInstanceForRedo(serviceName, groupName string, instance model.Instance) {
	key := util.GetGroupName(serviceName, groupName)
	c.registeredInstanceCached.Store(key, instance)
}

func (c *ConnectionEventListener) CacheInstancesForRedo(serviceName, groupName string, instances []model.Instance) {
	key := util.GetGroupName(serviceName, groupName)
	c.registeredInstanceCached.Store(key, instances)
}

func (c *ConnectionEventListener) RemoveInstanceForRedo(serviceName, groupName string, instance model.Instance) {
	key := util.GetGroupName(serviceName, groupName)
	c.registeredInstanceCached.Delete(key)
}

// subscriberRedoEntry tracks a cached subscription redo intent along with
// whether the subscription has been confirmed by the server. It aligns with
// the Java SDK SubscriberRedoData: the redo intent is cached before the
// subscribe request is sent, while the registered state is only confirmed
// after a successful subscribe response.
type subscriberRedoEntry struct {
	registered atomic.Bool
}

func (c *ConnectionEventListener) CacheSubscriberForRedo(fullServiceName, clusters string) {
	key := util.GetServiceCacheKey(fullServiceName, clusters)
	if !c.IsSubscriberCached(key) {
		c.subscribes.Store(key, &subscriberRedoEntry{})
	}
}

func (c *ConnectionEventListener) IsSubscriberCached(key string) bool {
	_, ok := c.subscribes.Load(key)
	return ok
}

// SubscriberRegistered marks the cached redo entry as confirmed after a
// successful subscribe response.
func (c *ConnectionEventListener) SubscriberRegistered(fullServiceName, clusters string) {
	if entry, ok := c.subscribes.Load(util.GetServiceCacheKey(fullServiceName, clusters)); ok {
		if redoEntry, ok := entry.(*subscriberRedoEntry); ok {
			redoEntry.registered.Store(true)
		}
	}
}

// IsSubscriberRegistered reports whether the subscription has been confirmed
// by the server. A redo entry cached before a failed subscribe request is
// not considered registered.
func (c *ConnectionEventListener) IsSubscriberRegistered(key string) bool {
	entry, ok := c.subscribes.Load(key)
	if !ok {
		return false
	}
	redoEntry, ok := entry.(*subscriberRedoEntry)
	return ok && redoEntry.registered.Load()
}

func (c *ConnectionEventListener) RemoveSubscriberForRedo(fullServiceName, clusters string) {
	c.subscribes.Delete(util.GetServiceCacheKey(fullServiceName, clusters))
}
