#include "page/bitmap_page.h"

template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  size_t i;
  for (i = 0; i < page_allocated_; i++) {
    if (bytes[i >> 3] & (1 << (7 - (i % 8))))
      continue;
    else
      break;
  }
  if ((i == page_allocated_) && (page_allocated_ >> 3 == MAX_CHARS))
    return false;
  else {
    if (i == page_allocated_) {
      bytes[page_allocated_ >> 3] |= (1 << (7 - (page_allocated_ % 8)));
      page_offset = page_allocated_;
    } else {
      bytes[i >> 3] |= (1 << (7 - (i % 8)));
      page_offset = i;
    }
    page_allocated_++;
    return true;
  }
}

template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (bytes[page_offset >> 3] & (1 << (7 - (page_offset % 8)))) {
    bytes[page_offset >> 3] &= (~(1 << (7 - (page_offset % 8))));
    page_allocated_--;
    return true;
  } else
    return false;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if (bytes[page_offset >> 3] & (1 << (7 - (page_offset % 8))))
    return false;
  else
    return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;