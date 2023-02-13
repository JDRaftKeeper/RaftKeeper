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

#include <cerrno>
#include <vector>
#include <memory>

#include <Poco/Version.h>
#include <Poco/Exception.h>

#include <Common/StackTrace.h>

#include <fmt/format.h>

#if !defined(NDEBUG) || defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER) || defined(MEMORY_SANITIZER) || defined(UNDEFINED_BEHAVIOR_SANITIZER)
#define ABORT_ON_LOGICAL_ERROR
#endif

namespace Poco { class Logger; }


namespace RK
{

class Exception : public Poco::Exception
{
public:
    Exception() = default;
    Exception(const std::string & msg, int code, bool remote_ = false);
    Exception(const std::string & msg, const Exception & nested, int code);

    Exception(int code, const std::string & message)
        : Exception(message, code)
    {}

    // Format message with fmt::format, like the logging functions.
    template <typename ...Args>
    Exception(int code, const std::string & fmt, Args&&... args)
        : Exception(fmt::format(fmt, std::forward<Args>(args)...), code)
    {}

    struct CreateFromPocoTag {};
    struct CreateFromSTDTag {};

    Exception(CreateFromPocoTag, const Poco::Exception & exc);
    Exception(CreateFromSTDTag, const std::exception & exc);

    Exception * clone() const override { return new Exception(*this); }
    void rethrow() const override { throw *this; }
    const char * name() const throw() override { return "RK::Exception"; }
    const char * what() const throw() override { return message().data(); }

    /// Add something to the existing message.
    template <typename ...Args>
    void addMessage(const std::string& format, Args&&... args)
    {
        extendedMessage(fmt::format(format, std::forward<Args>(args)...));
    }

    void addMessage(const std::string& message)
    {
        extendedMessage(message);
    }

    /// Used to distinguish local exceptions from the one that was received from remote node.
    void setRemoteException(bool remote_ = true) { remote = remote_; }
    bool isRemoteException() const { return remote; }

    std::string getStackTraceString() const;

private:
#ifndef STD_EXCEPTION_HAS_STACK_TRACE
    StackTrace trace;
#endif
    bool remote = false;

    const char * className() const throw() override { return "RK::Exception"; }
};


std::string getExceptionStackTraceString(const std::exception & e);


/// Contains an additional member `saved_errno`. See the throwFromErrno function.
class ErrnoException : public Exception
{
public:
    ErrnoException(const std::string & msg, int code, int saved_errno_, const std::optional<std::string> & path_ = {})
        : Exception(msg, code), saved_errno(saved_errno_), path(path_) {}

    ErrnoException * clone() const override { return new ErrnoException(*this); }
    void rethrow() const override { throw *this; }

    int getErrno() const { return saved_errno; }
    const std::optional<std::string> getPath() const { return path; }

private:
    int saved_errno;
    std::optional<std::string> path;

    const char * name() const throw() override { return "RK::ErrnoException"; }
    const char * className() const throw() override { return "RK::ErrnoException"; }
};


/// Special class of exceptions, used mostly in ParallelParsingInputFormat for
/// more convenient calculation of problem line number.
class ParsingException : public Exception
{
public:
    ParsingException();
    ParsingException(const std::string & msg, int code);
    ParsingException(int code, const std::string & message);

    // Format message with fmt::format, like the logging functions.
    template <typename ...Args>
    ParsingException(int code, const std::string & fmt, Args&&... args)
        : Exception(fmt::format(fmt, std::forward<Args>(args)...), code)
    {}


    std::string displayText() const
#if defined(POCO_CLICKHOUSE_PATCH)
    override
#endif
    ;

    int getLineNumber() { return line_number_; }
    void setLineNumber(int line_number) { line_number_ = line_number;}

private:
    ssize_t line_number_{-1};
    mutable std::string formatted_message_;

    const char * name() const throw() override { return "RK::ParsingException"; }
    const char * className() const throw() override { return "RK::ParsingException"; }
};


using Exceptions = std::vector<std::exception_ptr>;


[[noreturn]] void throwFromErrno(const std::string & s, int code, int the_errno = errno);
/// Useful to produce some extra information about available space and inodes on device
[[noreturn]] void throwFromErrnoWithPath(const std::string & s, const std::string & path, int code,
                                         int the_errno = errno);


/** Try to write an exception to the log (and forget about it).
  * Can be used in destructors in the catch-all block.
  */
void tryLogCurrentException(const char * log_name, const std::string & start_of_message = "");
void tryLogCurrentException(Poco::Logger * logger, const std::string & start_of_message = "");


/** Prints current exception in canonical format.
  * with_stacktrace - prints stack trace for RK::Exception.
  * check_embedded_stacktrace - if RK::Exception has embedded stacktrace then
  *  only this stack trace will be printed.
  * with_extra_info - add information about the filesystem in case of "No space left on device" and similar.
  */
std::string getCurrentExceptionMessage(bool with_stacktrace, bool check_embedded_stacktrace = false,
                                       bool with_extra_info = true);

/// Returns error code from ErrorCodes
int getCurrentExceptionCode();


/// An execution status of any piece of code, contains return code and optional error
struct ExecutionStatus
{
    int code = 0;
    std::string message;

    ExecutionStatus() = default;

    explicit ExecutionStatus(int return_code, const std::string & exception_message = "")
    : code(return_code), message(exception_message) {}

    static ExecutionStatus fromCurrentException(const std::string & start_of_message = "");

    std::string serializeText() const;

    void deserializeText(const std::string & data);

    bool tryDeserializeText(const std::string & data);
};


void tryLogException(std::exception_ptr e, const char * log_name, const std::string & start_of_message = "");
void tryLogException(std::exception_ptr e, Poco::Logger * logger, const std::string & start_of_message = "");

std::string getExceptionMessage(const Exception & e, bool with_stacktrace, bool check_embedded_stacktrace = false);
std::string getExceptionMessage(std::exception_ptr e, bool with_stacktrace);


void rethrowFirstException(const Exceptions & exceptions);


template <typename T>
std::enable_if_t<std::is_pointer_v<T>, T> exception_cast(std::exception_ptr e)
{
    try
    {
        std::rethrow_exception(std::move(e));
    }
    catch (std::remove_pointer_t<T> & concrete)
    {
        return &concrete;
    }
    catch (...)
    {
        return nullptr;
    }
}

}
