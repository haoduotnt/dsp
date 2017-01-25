package dsp_flights

import (
	"crypto/cipher"
	"encoding/base64"
	"golang.org/x/crypto/blowfish"
	"strings"
	//https://github.com/golang/go/blob/master/src/crypto/cipher/cbc.go
	//Blowfish CBC?
	//r = block.NewCBCDecrypter(blowfish.NewCipher(key), iv, r)
	//from https://talks.golang.org/2010/io/talk.pdf
)

type B64 struct {
	Key []byte
	IV  []byte
}

func (b *B64) Encrypt(pt []byte) string {
	key := b.Key
	block, err := blowfish.NewCipher(key)
	if err != nil {
		panic(err.Error())
	}
	mode := cipher.NewCBCEncrypter(block, b.IV)
	// padder := padding.NewPkcs5Padding()
	// pt, err = padder.Pad(pt) // padd last block of plaintext if block size less than block cipher size
	// if err != nil {
	// 	panic(err.Error())
	// }
	r := 8 - (len(pt) % 8)
	pt = append(pt, make([]byte, r)...)
	ct := make([]byte, len(pt))
	mode.CryptBlocks(ct, pt)
	sEnc := base64.StdEncoding.EncodeToString([]byte(ct))
	// log.Println("pre replace", sEnc)
	sEnc = strings.Replace(sEnc, "+", "-", -1)
	sEnc = strings.Replace(sEnc, "/", "_", -1)
	sEnc = strings.Replace(sEnc, "=", ".", -1)
	return sEnc
}

func (b *B64) Decrypt(ct string) []byte {
	key := b.Key
	ct = strings.Replace(ct, "-", "+", -1)
	ct = strings.Replace(ct, "_", "/", -1)
	ct = strings.Replace(ct, ".", "=", -1)
	// log.Println("post replace", ct)
	sDec, _ := base64.StdEncoding.DecodeString(ct)
	block, err := blowfish.NewCipher(key)
	if err != nil {
		panic(err.Error())
	}
	mode := cipher.NewCBCDecrypter(block, b.IV)
	pt := make([]byte, len(sDec))
	mode.CryptBlocks(pt, sDec)

	// log.Println("dump", pt)
	// r := bytes.LastIndex(pt, []byte{0})
	// log.Println("width", r)
	// pt = pt[0:r]
	return pt
}

func (b *B64) GetCT(ct string) string {
	return b.Encrypt([]byte(ct))
}
