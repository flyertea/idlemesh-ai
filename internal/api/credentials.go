package api

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const workerCredentialPrefix = "idm_"

type WorkerCredentialChange struct {
	WorkerID string `json:"worker_id"`
	Digest   string `json:"digest,omitempty"`
}

func ValidWorkerCredentialDigest(digest string) bool {
	if len(digest) != 2*sha256.Size {
		return false
	}
	for i := range len(digest) {
		char := digest[i]
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func NewWorkerCredential() (token, digest string, err error) {
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return "", "", err
	}
	token = workerCredentialPrefix + hex.EncodeToString(secret[:])
	digest, _ = WorkerCredentialDigest(token)
	return token, digest, nil
}

func WorkerCredentialDigest(token string) (string, bool) {
	encoded, ok := strings.CutPrefix(token, workerCredentialPrefix)
	if !ok || !ValidWorkerCredentialDigest(encoded) {
		return "", false
	}
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:]), true
}
