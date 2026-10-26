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

package util

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyncMapLen(t *testing.T) {
	var m sync.Map
	assert.Equal(t, 0, SyncMapLen(&m))

	m.Store("a", 1)
	m.Store("b", 2)
	m.Store("a", 3)
	assert.Equal(t, 2, SyncMapLen(&m))

	m.Delete("a")
	assert.Equal(t, 1, SyncMapLen(&m))
}
