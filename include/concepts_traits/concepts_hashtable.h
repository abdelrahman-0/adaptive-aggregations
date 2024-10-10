#pragma once

namespace concepts {

template <typename T>
concept is_slot = (std::is_integral_v<T> or std::is_pointer_v<T>);

}
