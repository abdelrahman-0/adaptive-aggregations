#pragma once

#include "common/page.h"
#include "defaults.h"

template <typename... Attributes>
using PageLocal = Page<defaults::local_page_size, Attributes...>;

static_assert(sizeof(PageLocal<int32_t>) == defaults::local_page_size);
static_assert(sizeof(PageLocal<int32_t, char[20]>) == defaults::local_page_size);
static_assert(sizeof(PageLocal<int64_t, int64_t, int32_t, unsigned char[4]>) == defaults::local_page_size);
