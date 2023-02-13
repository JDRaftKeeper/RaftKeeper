/**
 * Copyright 2016-2023 ClickHouse, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <Common/getHashOfLoadedBinary.h>

#if defined(__linux__)

#include <link.h>
#include <array>
#include <Common/hex.h>


static int callback(dl_phdr_info * info, size_t, void * data)
{
    SipHash & hash = *reinterpret_cast<SipHash*>(data);

    for (size_t header_index = 0; header_index < info->dlpi_phnum; ++header_index)
    {
        const auto & phdr = info->dlpi_phdr[header_index];

        if (phdr.p_type == PT_LOAD && (phdr.p_flags & PF_X))
        {
            hash.update(phdr.p_filesz);
            hash.update(reinterpret_cast<const char *>(info->dlpi_addr + phdr.p_vaddr), phdr.p_filesz);
        }
    }

    return 1;   /// Do not continue iterating.
}


SipHash getHashOfLoadedBinary()
{
    SipHash hash;
    dl_iterate_phdr(callback, &hash);
    return hash;
}


std::string getHashOfLoadedBinaryHex()
{
    SipHash hash = getHashOfLoadedBinary();
    std::array<UInt64, 2> checksum;
    hash.get128(checksum[0], checksum[1]);
    return getHexUIntUppercase(checksum);
}

#else

SipHash getHashOfLoadedBinary()
{
    return {};
}


std::string getHashOfLoadedBinaryHex()
{
    return {};
}

#endif
