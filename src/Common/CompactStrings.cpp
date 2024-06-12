#include <Common/CompactStrings.h>


namespace RK
{

CompactStrings::CompactStrings(CompactStrings && other) : data(std::move(other.data)), offsets(std::move(other.offsets))
{
}

CompactStrings::CompactStrings(const CompactStrings & other)
    : data(other.data.begin(), other.data.end()), offsets(other.offsets.begin(), other.offsets.end())
{
}

void CompactStrings::reserve(size_t n, size_t total_size_)
{
    offsets.reserve(n);
    if (total_size_)
        data.reserve(total_size_);
    else
        data.reserve(AVG_ELEMENT_SIZE_HINT * total_size_);
}

StringRef CompactStrings::operator[](int64_t i) const
{
    return StringRef(&data[offsets[i - 1]], offsets[i] - offsets[i - 1]);
}

String CompactStrings::getString(int64_t i) const
{
    return String(&data[offsets[i - 1]], offsets[i] - offsets[i - 1]);
}

Strings CompactStrings::toStrings() const
{
    Strings ret(offsets.size());
    for (size_t i = 1; i < offsets.size(); ++i)
        ret[i - 1] = String(&data[offsets[i - 1]], offsets[i] - offsets[i - 1]);
    return ret;
}

}
