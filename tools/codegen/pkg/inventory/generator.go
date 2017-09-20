// Copyright 2017 Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package inventory is used to generate the mixer adapter inventory source
// file.
package inventory // import "istio.io/mixer/tools/codegen/pkg/inventory"

import (
	"bytes"
	"fmt"
	"go/format"
	"io"
	"sort"
	"text/template"

	"golang.org/x/tools/imports"
)

var inventoryTmpl = `// Copyright 2017 Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// THIS FILE IS AUTOMATICALLY GENERATED.

package adapter

import(
		{{range .}}{{.Package}} "{{.Path}}"; {{end}}
        adptr "istio.io/mixer/pkg/adapter"
)

// Inventory returns the inventory of all available adapters.
func Inventory() []adptr.InfoFn {
	return []adptr.InfoFn{
{{range .}}{{.Package}}.GetInfo,
{{end}}
	}
}
`

type model []Import

// Import holds the full import path and short package name
// of an adapter that will be part of the generated inventory.
type Import struct {
	Path    string
	Package string
}

// Len is part of sort.Interface.
func (m model) Len() int {
	return len(m)
}

// Swap is part of sort.Interface.
func (m model) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

// Less is part of sort.Interface.
func (m model) Less(i, j int) bool {
	return m[i].Package < m[j].Package
}

// Generate takes a map (package -> import path) and generates the
// source code to use as the adapter inventory for Mixer.
func Generate(packageMap map[string]string, out io.Writer) error {
	tmpl, err := template.New("InventoryTmpl").Parse(inventoryTmpl)
	if err != nil {
		return fmt.Errorf("could not load template: %v", err)
	}

	m := make(model, 0, len(packageMap))
	for pkg, path := range packageMap {
		m = append(m, Import{path, pkg})
	}

	sort.Sort(m)

	buf := new(bytes.Buffer)
	if err = tmpl.Execute(buf, m); err != nil {
		return fmt.Errorf("could not generate inventory code: %v (%s)", err, buf.Bytes())
	}

	fmtd, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("could not format generated code: %v (%s)", err, buf.Bytes())
	}

	imports.LocalPrefix = "istio.io"
	imptd, err := imports.Process("", fmtd, &imports.Options{FormatOnly: true, Comments: true})
	if err != nil {
		return fmt.Errorf("could not fix imports for generated code: %v", err)
	}

	_, err = out.Write(imptd)
	return err
}
