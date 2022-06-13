package main

import "net/url"

func formatURI(commonURI string) (uri string, err error) {
	u, err := url.ParseRequestURI(commonURI)
	if err != nil {
		return
	}

	formated := url.URL{
		Scheme: u.Scheme,
		Host:   u.Host,
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

	// for cdk we need to use unescaped prefix
	formated.RawQuery, err = url.QueryUnescape(v.Encode())
	if err != nil {
		return
	}

	return formated.String(), nil
}
