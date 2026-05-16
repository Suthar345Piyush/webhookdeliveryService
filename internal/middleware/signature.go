// HMAC sha256 signature  verification
// a fiber middleware

package middleware

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/suthar345piyush/internal/logger"
	"go.uber.org/zap"
)

// X-Hub-Signature-256 is an security mechanism to verify that incoming webhook request is coming from genuine platform like github, etc..

// secret is an HMAC secret, this will shared between our system and git server
// verify signature function will returns an fiber middleware that validates the X-Hub-Signature-256 header on every incoming request

func VerifySignature(secret string) fiber.Handler {
	return func(c fiber.Ctx) error {

		// we will read the raw body first before parsing
		rawBody := c.Body()

		sigHeader := c.Get("X-Hub-Signature-256")

		if sigHeader == "" {
			logger.Log.Warn("request rejected: missing X-Hub-Signature-256", zap.String("ip", c.IP()))

			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "missing signature header",
			})
		}

		// header format should be like - sha256=<hex>, and strip it's prefix

		sigHex := strings.TrimPrefix(sigHeader, "sha256=")

		if sigHex == sigHeader {
			// this is wrong/invalid header

			logger.Log.Warn("request rejected: malformed signature header", zap.String("header", sigHeader))

			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "malformed signature header",
			})
		}

		if !validSignature([]byte(secret), rawBody, sigHex) {
			logger.Log.Warn("request rejected: signature mistmatch", zap.String("ip", c.IP()))

			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "invalid signature",
			})

		}

		c.Locals("rawBody", rawBody)

		return c.Next()

	}
}

// valid signature function , it compute HMAC-sha256(secret, body) and compare it to the provided hex string using constant-time comparison, constant-time comparison because if we use byte-to-byte comparison, then that will break on the first mismatch, which leaks the timing information, and that attackers can use to forge signature incrementally

func validSignature(secert, body []byte, receivedHex string) bool {

	mac := hmac.New(sha256.New, secert)

	mac.Write(body)

	expected := hex.EncodeToString(mac.Sum(nil))

	// using subtle.ConstantTimeComparison, and it require equal size of slices to compare so converting both of them into []byte

	return subtle.ConstantTimeCompare(

		[]byte(fmt.Sprintf("sha256=%s", expected)), []byte(fmt.Sprintf("sha256=%s", receivedHex)),
	) == 1

}
