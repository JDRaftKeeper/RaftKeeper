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
#pragma once

#include <common/types.h>

#include <string>
#include <vector>
#include <array>
#include <optional>
#include <functional>
#include <signal.h>

#ifdef __APPLE__
// ucontext is not available without _XOPEN_SOURCE
#   pragma clang diagnostic ignored "-Wreserved-id-macro"
#   define _XOPEN_SOURCE 700
#endif
#include <ucontext.h>

struct NoCapture
{
};

/// Tries to capture current stack trace using libunwind or signal context
/// NOTE: StackTrace calculation is signal safe only if updatePHDRCache() was called beforehand.
class StackTrace
{
public:
    struct Frame
    {
        const void * virtual_addr = nullptr;
        void * physical_addr = nullptr;
        std::optional<std::string> symbol;
        std::optional<std::string> object;
        std::optional<std::string> file;
        std::optional<UInt64> line;
    };

    static constexpr size_t capacity =
#ifndef NDEBUG
        /* The stacks are normally larger in debug version due to less inlining.
         *
         * NOTE: it cannot be larger then 56 right now, since otherwise it will
         * not fit into minimal PIPE_BUF (512) in TraceCollector.
         */
        56
#else
        32
#endif
        ;
    using FramePointers = std::array<void *, capacity>;
    using Frames = std::array<Frame, capacity>;

    /// Tries to capture stack trace
    StackTrace();

    /// Tries to capture stack trace. Fallbacks on parsing caller address from
    /// signal context if no stack trace could be captured
    explicit StackTrace(const ucontext_t & signal_context);

    /// Creates empty object for deferred initialization
    explicit StackTrace(NoCapture);

    size_t getSize() const;
    size_t getOffset() const;
    const FramePointers & getFramePointers() const;
    std::string toString() const;

    static std::string toString(void ** frame_pointers, size_t offset, size_t size);
    static void symbolize(const FramePointers & frame_pointers, size_t offset, size_t size, StackTrace::Frames & frames);

    void toStringEveryLine(std::function<void(const std::string &)> callback) const;

protected:
    void tryCapture();

    size_t size = 0;
    size_t offset = 0;  /// How many frames to skip while displaying.
    FramePointers frame_pointers{};
};

std::string signalToErrorMessage(int sig, const siginfo_t & info, const ucontext_t & context);
