// Copyright 2013 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef URL_URL_UTIL_INTERNAL_HH_
#define URL_URL_UTIL_INTERNAL_HH_

#include <string>

#include "url/url_parse.hh"

namespace url {

// Given a string and a range inside the string, compares it to the given
// lower-case |compare_to| buffer.
bool CompareSchemeComponent(const char* spec,
                            const Component& component,
                            const char* compare_to);

}  // namespace url

#endif  // URL_URL_UTIL_INTERNAL_HH_
