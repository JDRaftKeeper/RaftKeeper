if(NOT EXISTS "${RaftKeeper_SOURCE_DIR}/contrib/boringssl/README.md")
    if(USE_INTERNAL_SSL_LIBRARY)
        message(WARNING "submodule contrib/boringssl is missing. to fix try run: \n git submodule update --init --recursive")
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal ssl library")
    endif()
    set(MISSING_INTERNAL_SSL_LIBRARY 1)
endif()

set (OPENSSL_ROOT_DIR "${RaftKeeper_SOURCE_DIR}/contrib/boringssl")

if (ARCH_AMD64)
    set (OPENSSL_INCLUDE_DIR "${OPENSSL_ROOT_DIR}/include")
elseif (ARCH_AARCH64)
    set (OPENSSL_INCLUDE_DIR "${OPENSSL_ROOT_DIR}/include")
endif ()
set (OPENSSL_CRYPTO_LIBRARY crypto)
set (OPENSSL_SSL_LIBRARY ssl)
set (OPENSSL_FOUND 1)
set (OPENSSL_LIBRARIES ${OPENSSL_SSL_LIBRARY} ${OPENSSL_CRYPTO_LIBRARY})

if(OPENSSL_FOUND)
    # we need keep OPENSSL_FOUND for many libs in contrib
    set(USE_SSL 1)
endif()

message (STATUS "Using ssl=${USE_SSL}: ${OPENSSL_INCLUDE_DIR} : ${OPENSSL_LIBRARIES}")
