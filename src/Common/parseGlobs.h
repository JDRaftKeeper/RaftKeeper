#pragma once
#include <string>
#include <vector>

namespace RK
{
/* Parse globs in string and make a regexp for it.
 */
std::string makeRegexpPatternFromGlobs(const std::string & initial_str_with_globs);
}
