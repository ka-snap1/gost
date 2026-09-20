/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package consistent

import (
	"fmt"
	"testing"
)

var benchmarkRingHash *Consistent

func BenchmarkFullBuild(b *testing.B) {
	for _, hostCount := range []int{1, 32, 256, 1024} {
		for _, replicaCount := range []int{10, 100} {
			name := fmt.Sprintf("hosts=%d/replicas=%d", hostCount, replicaCount)
			b.Run(name, func(b *testing.B) {
				// Prepare input outside the timed loop: each operation builds a new ring.
				hosts := make([]string, hostCount)
				for i := range hosts {
					hosts[i] = fmt.Sprintf("127.0.0.1:%d", 8000+i)
				}

				b.Run("Add", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						ring := NewConsistentHash(WithReplicaNum(replicaCount))
						for _, host := range hosts {
							ring.Add(host)
						}
						benchmarkRingHash = ring
					}
				})
				b.Run("AddBatch", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						ring := NewConsistentHash(WithReplicaNum(replicaCount))
						ring.AddBatch(hosts)
						benchmarkRingHash = ring
					}
				})
			})
		}
	}
}
