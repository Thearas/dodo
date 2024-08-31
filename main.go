/*
Copyright © 2024 Thearas thearas850@gmail.com

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"os"
	"runtime/pprof"

	"github.com/Thearas/dodo/cmd"
)

func main() {
	if os.Getenv("PPROF") != "" {
		cpuF, perr := os.Create("cpu.pprof")
		if perr != nil {
			panic(perr)
		}
		memF, perr := os.Create("mem.pprof")
		if perr != nil {
			panic(perr)
		}
		_ = pprof.StartCPUProfile(cpuF)
		defer func() {
			pprof.StopCPUProfile()
			// runtime.GC()
			_ = pprof.WriteHeapProfile(memF)
		}()
	}

	cmd.Execute()
}
