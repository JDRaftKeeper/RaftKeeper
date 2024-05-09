#pragma once

#ifdef __SSE2__
#    include <emmintrin.h>
#endif

#if defined(__aarch64__) && defined(__ARM_NEON)
#    include <arm_neon.h>
#    include <cstring>
#    pragma clang diagnostic ignored "-Wreserved-identifier"
#endif

/** Memory copy with SIMD instructions. It is used in places where performance is critical for small blocks of memory.
  */
namespace RK
{
#ifdef __SSE2__

inline void memcopy(char * __restrict dst, const char * __restrict src, size_t n)
{
    auto aligned_n = n / 16 * 16;
    auto left = n - aligned_n;
    while (aligned_n > 0)
    {
        _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), _mm_loadu_si128(reinterpret_cast<const __m128i *>(src)));

        dst += 16;
        src += 16;
        aligned_n -= 16;

        /// Avoid clang loop-idiom optimization, which transforms _mm_storeu_si128 to built-in memcpy
        __asm__ __volatile__("" : : : "memory");
    }
    ::memcpy(dst, src, left);
}

#elif defined(__aarch64__) && defined(__ARM_NEON)

inline void memcopy(char * __restrict dst, const char * __restrict src, size_t n)
{
    auto aligned_n = n / 16 * 16;
    auto left = n - aligned_n;
    while (aligned_n > 0)
    {
        vst1q_s8(reinterpret_cast<signed char *>(dst), vld1q_s8(reinterpret_cast<const signed char *>(src)));

        dst += 16;
        src += 16;
        aligned_n -= 16;
    }
    ::memcpy(dst, src, left);
}

#else

inline void memcopy(void * __restrict dst, const void * __restrict src, size_t n)
{
    memcpy(dst, src, n);
}

#endif
}
