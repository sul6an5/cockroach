#include "textflag.h"

// func GetTSC() uint64
TEXT ·GetTSC(SB),NOSPLIT,$0-8
	RDTSC
	SHLQ	$32, DX
	ADDQ	DX, AX
	MOVQ	AX, ret+0(FP)
	RET
