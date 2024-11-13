// Zipfian workload generator
// incorporated from TurboDB

package ycsbt

import (
	"errors"
	"math"
	"time"

	"golang.org/x/exp/rand"
)

var TaylorThreshold = 1e-8 // Threshold below which Taylor series will be used
var F12 = 1.0 / 2
var F13 = 1.0 / 3
var F14 = 1.0 / 4

type RejectionInversionGenerator struct {
	// seq                  *sequence
	numElements          uint64  // number of elements
	exponent_            float64 // exponent parameter of the distribution
	hIntegralX1          float64 // hIntegral(1.5) - 1.
	hIntegralNumElements float64 // hIntegral(numElements + 0.5)
	s_                   float64 // 2 - hIntegralInv(hIntegral(2.5) - h(2)
	random               *rand.Rand
}

// func (g *RejectionInversionGenerator) sequence() int64 {
//	return atomic.LoadInt64(&g.seq.val)
// }

func (z *RejectionInversionGenerator) IncrementIMax(count uint64) error {
	// z.mu.Lock()
	// defer z.mu.Unlock()
	// z.mu.iMax += count
	return nil
}

func (g *RejectionInversionGenerator) hIntegral(x float64) float64 {
	logX := math.Log(x)
	return g.helper2((1-g.exponent_)*logX) * logX
}

func (g *RejectionInversionGenerator) helper2(x float64) float64 {
	if math.Abs(x) > TaylorThreshold {
		return math.Expm1(x) / x
	} else {
		return 1 + x*F12*(1+x*F13*(1+F14*x))
	}
}

func NewRejectionInversionGenerator(numElements uint64,
	exponent float64) (*RejectionInversionGenerator, error) {
	if numElements <= 0 {
		return &RejectionInversionGenerator{}, errors.New("key range non-positive")
	}

	if exponent < 0 || exponent == 1.0 {
		return &RejectionInversionGenerator{}, errors.New("zipfian constant invalid")
	}

	g := RejectionInversionGenerator{
		//seq: seq,
		numElements: numElements,
		exponent_:   exponent,
		random:      rand.New(rand.NewSource(uint64(time.Now().UnixNano()))),
	}
	g.hIntegralX1 = g.hIntegral(1.5) - 1
	g.hIntegralNumElements = g.hIntegral(float64(numElements) + F12)
	g.s_ = 2 - g.hIntegralInv(g.hIntegral(2.5)-g.h(2))

	return &g, nil
}

func (g *RejectionInversionGenerator) Uint64() uint64 {
	for {
		var u = g.hIntegralNumElements + g.U()*(g.
			hIntegralX1-g.hIntegralNumElements)
		// u is uniformly distributed in (h_integral_x1_, h_integral_num_elements]

		var x = g.hIntegralInv(u)
		var k = uint64(x + F12)

		// Limit k to the range [1, num_elements_] if it would be outside due
		// to numerical inaccuracies
		if k < 1 {
			k = 1
		} else if k > g.numElements {
			k = g.numElements
		}

		if float64(k)-x <= g.s_ || u >= g.hIntegral(float64(k)+F12)-g.h(
			float64(k)) {
			return k
		}
	}
}

func (g *RejectionInversionGenerator) U() float64 {
	return g.random.Float64()
}

func (g *RejectionInversionGenerator) h(x float64) float64 {
	return math.Exp(-g.exponent_ * math.Log(x))
}
func (g *RejectionInversionGenerator) helper1(x float64) float64 {
	if math.Abs(x) > TaylorThreshold {
		return math.Log1p(x) / x
	} else {
		return 1 - x*(F12-x*(F13-F14*x))
	}
}

func (g *RejectionInversionGenerator) hIntegralInv(x float64) float64 {
	var t = x * (1 - g.exponent_)
	if t < -1 {
		// Limit value to the range [-1, +inf).
		// t could be smaller than -1 in some rare cases due to numerical errors.
		t = -1
	}
	return math.Exp(g.helper1(t) * x)
}

/*
func (g *RejectionInversionGenerator) readKey() int64 {
	return int64(g.sample())
}

func (g *RejectionInversionGenerator) writeKey() int64 {
	return int64(g.sample())
}
*/

func (g *RejectionInversionGenerator) rand() *rand.Rand {
	return g.random
}
