#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) { num_pages_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
    latch.lock();
    if (list_noVisit.empty())  //如果没有可替换(删除)页
    {
        latch.unlock();
        return false;
    }
    else
    {
        *frame_id = list_noVisit.back();  //list中最前面的应该是最早插入的，即使用最少的
        map_noVisit.erase(*frame_id);
        list_noVisit.pop_back();
        latch.unlock();
        return true;
    }
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    latch.lock();
    if (map_noVisit.find(frame_id) == map_noVisit.end())
    {
        latch.unlock();
        return;
    }
    list_noVisit.erase(map_noVisit[frame_id]);
    map_noVisit.erase(frame_id);
    latch.unlock();
    return;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    latch.lock();
    if (map_noVisit.find(frame_id) != map_noVisit.end())
    {
        latch.unlock();
        return;
    }
    list_noVisit.push_front(frame_id);
    map_noVisit[frame_id] = list_noVisit.begin();
    latch.unlock();
    return;
}

size_t LRUReplacer::Size() {
    size_t size;
    size = list_noVisit.size();
    return size;
}