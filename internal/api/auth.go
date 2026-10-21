package api

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// requireAuth wraps a handler with Bearer token authentication. On failure it
// returns 401 with a WWW-Authenticate header.
func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// authorized checks the Authorization header against the configured token in
// constant time. An empty configured token disables authentication.
func (s *Server) authorized(r *http.Request) bool {
	token := s.tokenValue()
	if token == "" {
		return true
	}

	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return false
	}
	provided := strings.TrimPrefix(header, "Bearer ")
	return subtle.ConstantTimeCompare([]byte(provided), []byte(token)) == 1
}
