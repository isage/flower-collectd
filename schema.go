package main

import (
  "text/template"
  "bytes"
  "strings"
  "net"
  "encoding/binary"
  "time"
  "github.com/calmh/ipfix"
)

type Field struct {
  Name    string
  Type    string
  From    string
  Default string
}

type Table struct {
    Fields    []Field
    Type      string
    PKey      string
    Keys      []string
    Granulate int
    Name      string
    Db        string
}

type Schema struct {
    Tables []Table
}

func (t Table) Marshal() string {
  const createTbl = `
    CREATE TABLE IF NOT EXISTS {{.Db}}.{{.Name}} (
      {{ range $index, $element := .Fields}}{{if $index}}, {{end}}{{$element.Name}} {{$element.Type}}{{if $element.Default}} DEFAULT {{$element.Default}}{{end}}{{end}}
    ) ENGINE={{.Type}}({{.PKey}}, ({{ join .Keys ", " }}), {{.Granulate}});
  `
  tpl := template.Must(template.New("create_tbl").Funcs(template.FuncMap{"join": strings.Join}).Parse(createTbl))
  var out bytes.Buffer
  tpl.Execute(&out, t)
  return out.String()
}

func (t Table) GetColumns() []string {
    vsf := make([]string, 0)
    for _, v := range t.Fields {
        if v.From != "" {
            vsf = append(vsf, v.Name)
        }
    }
    return vsf
}

func findField(fieldList []ipfix.InterpretedField, name string) ipfix.InterpretedField {
  for _, v := range fieldList {
    if v.Name == name {
      return v
    }
  }
  return ipfix.InterpretedField{name, 0,0,0,nil}
}


func FilterFields(in []Field, fieldList []ipfix.InterpretedField) []interface{} {
    vsf := make([]interface{}, 0)
    for _, v := range in {
        if v.From != "" {
            field := findField(fieldList, v.From)
            if (field.FieldID == 8) || (field.FieldID == 12) {
              vsf = append(vsf, binary.BigEndian.Uint32(*field.Value.(*net.IP)))        // ipfix returns IP as net.IP, but we need uint32 in clickhouse
            } else if (field.FieldID == 152) || (field.FieldID == 153) {
              vsf = append(vsf, field.Value.(time.Time).Format("2006-01-02 15:04:05")) // go-clickhouse has a bug with time.Time
            } else {
              vsf = append(vsf, field.Value)
            }
        }
    }
    return vsf
}

func (t Table) PrepareRow(fieldList []ipfix.InterpretedField) []interface{} {

  fields := FilterFields(t.Fields, fieldList)
  return fields
}