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

package config_client

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type rateLimiterCheck struct {
	rateLimiterCache sync.Map // cache
}

var checker rateLimiterCheck

// IsLimited return true when request is limited
func IsLimited(checkKey string) bool {
	lm, exist := checker.rateLimiterCache.Load(checkKey)
	if !exist {
		// define a new limiter,allow 5 times per second,and reserve stock is 5.
		lm, _ = checker.rateLimiterCache.LoadOrStore(checkKey, rate.NewLimiter(rate.Limit(5), 5))
	}
	limiter := lm.(*rate.Limiter)
	add := time.Now().Add(time.Second)
	return !limiter.AllowN(add, 1)
}
