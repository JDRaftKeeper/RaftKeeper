#pragma once

#include "WriteBufferFromFileDescriptor.h"


namespace RK
{

/** Write to file descriptor but drop the data if write would block or fail.
  * To use within signal handler. Motivating example: a signal handler invoked during execution of malloc
  *  should not block because some mutex (or even worse - a spinlock) may be held.
  */
class WriteBufferFromFileDescriptorDiscardOnFailure : public WriteBufferFromFileDescriptor
{
protected:
    void nextImpl() override;

public:
    using WriteBufferFromFileDescriptor::WriteBufferFromFileDescriptor;
    ~WriteBufferFromFileDescriptorDiscardOnFailure() override {}
};

}
