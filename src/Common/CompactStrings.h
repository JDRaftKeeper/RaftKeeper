#pragma once

#include "PODArray.h"
#include "common/StringRef.h"


namespace RK
{

/**
 * Compact string list which store all data into single memory block to avoid the overhead of multiple allocations and de-allocations.
 */
class CompactStrings
{
public:
    using Offsets = PODArray<int64_t, 128, Allocator<false>, 15, 8>;
    using Data = PODArray<char, 256, Allocator<false>, 15, 8>;

    static constexpr size_t AVG_ELEMENT_SIZE_HINT = 64;

    struct Iterator
    {
        Offsets::const_iterator itr;
        const Data & data;

        Iterator(Offsets::const_iterator itr_, const Data & data_) : itr(itr_), data(data_) {}

        void operator++()
        {
            itr++;
        }

        bool operator!=(const Iterator & other) const
        {
            return itr != other.itr;
        }

        StringRef operator*() const
        {
            return StringRef(data.data() + *(itr - 1), *itr - *(itr - 1));
        }
    };

    CompactStrings() = default;

    CompactStrings(CompactStrings && other);
    CompactStrings(const CompactStrings & other);

    void reserve(size_t n, size_t total_size = 0);

    void push_back(const String & s);
    template <class Ttr> void push_back(Ttr begin, Ttr end)
    {
        for (Ttr it = begin; it != end; ++it)
            push_back(*it);
    }

    StringRef operator[](int64_t i) const;
    String getString(int64_t i) const;

    Strings toStrings() const;

    inline Iterator begin() const { return Iterator(offsets.begin(), data); }
    inline Iterator end() const { return Iterator(offsets.end(), data); }

    size_t size() const { return offsets.size(); }

private:
    Data data;
    Offsets offsets;
};

}
