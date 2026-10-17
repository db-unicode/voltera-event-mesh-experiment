package expctl

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// pgCreds son las credenciales de un Secret <instancia>-credentials.
type pgCreds struct{ User, Password, DB string }

func readPGCreds(ctx context.Context, kube Kubectl, ns, instance string) (pgCreds, error) {
	out, err := kube.Output(ctx, "-n", ns, "get", "secret", instance+"-credentials", "-o", "json")
	if err != nil {
		return pgCreds{}, err
	}
	var sec struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(out, &sec); err != nil {
		return pgCreds{}, err
	}
	dec := func(k string) (string, error) {
		v, ok := sec.Data[k]
		if !ok {
			return "", fmt.Errorf("secret %s-credentials no tiene la clave %s", instance, k)
		}
		b, err := base64.StdEncoding.DecodeString(v)
		return string(b), err
	}
	var c pgCreds
	if c.User, err = dec("POSTGRES_USER"); err != nil {
		return c, err
	}
	if c.Password, err = dec("POSTGRES_PASSWORD"); err != nil {
		return c, err
	}
	if c.DB, err = dec("POSTGRES_DB"); err != nil {
		return c, err
	}
	return c, nil
}

// csvToObjects convierte CSV con cabecera en una lista de objetos (números y NULL vacíos tipados).
func csvToObjects(data []byte) ([]map[string]any, error) {
	recs, err := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if err != nil {
		return nil, err
	}
	rows := []map[string]any{}
	if len(recs) == 0 {
		return rows, nil
	}
	for _, rec := range recs[1:] {
		obj := map[string]any{}
		for i, h := range recs[0] {
			if i >= len(rec) {
				break
			}
			if f, err := strconv.ParseFloat(rec[i], 64); err == nil && rec[i] != "" {
				obj[h] = f
			} else {
				obj[h] = rec[i]
			}
		}
		rows = append(rows, obj)
	}
	return rows, nil
}

func cmdSQL(ctx context.Context, args []string, stdout, stderr io.Writer, kube Kubectl) error {
	fs := newFlagSet("sql", stderr)
	asJSON := fs.Bool("json", false, "salida como lista JSON de objetos en vez de CSV")
	noHeader := fs.Bool("no-header", false, "CSV sin cabecera")
	container := fs.String("container", "postgres", "contenedor de Postgres en el pod")
	out := fs.String("out", "", "archivo de salida (por defecto stdout)")
	pos, err := parseFlags(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return usageErr(`uso: sql ns/statefulset "<query>"`)
	}
	if *asJSON && *noHeader {
		return usageErr("--json y --no-header son incompatibles")
	}
	ns, sts, err := splitNS(pos[0])
	if err != nil {
		return usageErr("%v", err)
	}
	creds, err := readPGCreds(ctx, kube, ns, sts)
	if err != nil {
		return err
	}
	psql := []string{"-n", ns, "exec", "statefulset/" + sts, "-c", *container, "--",
		"env", "PGPASSWORD=" + creds.Password, "psql", "-X", "-v", "ON_ERROR_STOP=1",
		"-U", creds.User, "-d", creds.DB, "--csv"}
	if *noHeader {
		psql = append(psql, "-t")
	}
	psql = append(psql, "-c", pos[1])
	raw, err := kube.Output(ctx, psql...)
	if err != nil {
		return err
	}
	if !*asJSON {
		return writeOut(*out, stdout, raw)
	}
	rows, err := csvToObjects(raw)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return err
	}
	return writeOut(*out, stdout, append(b, '\n'))
}
