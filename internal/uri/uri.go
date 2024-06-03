package uri

import (
	"net/url"
	"path/filepath"
)

func NormalizeURI(commonURI string) (uri string, err error) {
	u, err := url.ParseRequestURI(commonURI)
	if err != nil {
		return
	}
	if u.Scheme == "file" {
		return commonURI, nil
	}

	formated := url.URL{
		Scheme:   u.Scheme,
		Host:     u.Host,
		RawQuery: u.RawQuery,
	}

	if u.Path == "" {
		return formated.String(), nil
	}

	// stdlib will add prefix '/'
	if u.Path[0] == '/' {
		u.Path = u.Path[1:]
	}

	// make sure we end prefix with '/'
	if u.Path[len(u.Path)-1] != '/' {
		u.Path += "/"
	}

	v := url.Values{
		"prefix": []string{u.Path},
	}

	if region := u.Query().Get("region"); region != "" {
		v.Set("region", region)
	}

	// for cdk we need to use unescaped prefix
	formated.RawQuery, err = url.QueryUnescape(v.Encode())
	if err != nil {
		return
	}

	return formated.String(), nil
}

func Join(commonURI, path string) (string, error) {
	commonURI, err := NormalizeURI(commonURI)
	if err != nil {
		return "", err
	}

	u, err := url.ParseRequestURI(commonURI)
	if err != nil {
		return "", err
	}

	values := u.Query()
	values.Set("prefix", filepath.Join(values.Get("prefix"), path))
	u.RawQuery, err = url.QueryUnescape(values.Encode())
	if err != nil {
		return "", err
	}
	return u.String(), nil
}
