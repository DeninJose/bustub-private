// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <optional>
#include <algorithm>
#include <shared_mutex>
#include "common/config.h"
#include "common/exception.h"

// TODO: Make the whole thing thread-safe
namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 *
 * If you wish to refer to the original ARC paper, please note that there are
 * two changes in our implementation:
 * 1. When the size of mru_ equals the target size, we don't check
 * the last access as the paper did when deciding which list to evict from.
 * This is fine since the original decision is stated to be arbitrary.
 * 2. Entries that are not evictable are skipped. If all entries from the desired side
 * (mru_ / mfu_) are pinned, we instead try victimize the other side (mfu_ / mru_),
 * and move it to its corresponding ghost list (mfu_ghost_ / mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
    std::unique_lock writeLock(latch_);

    if (curr_size_ == 0) {
        return std::nullopt;
    }

    // There is atleast one evictable frame
    auto mru_size = mru_.size();
    frame_id_t frameId;

    typedef std::unordered_map<frame_id_t, std::shared_ptr<FrameStatus>> frame_status_map;
    auto rfind_evictable = [](std::list<frame_id_t>& linkedList, frame_status_map& frameStatusMap){
        auto itEvict = linkedList.rend();
        for (auto it = linkedList.rbegin(); it != linkedList.rend(); it++) {
            auto frameId = *it;
            auto frameStatus = frameStatusMap[frameId];
            if (frameStatus->evictable_) {
                itEvict = it;
                break;
            }
        }
        return itEvict;
    };

    /** If either mru or mfu contains no evictable frames, evict from the other one
     *  If mru size is greater than or equal to the target size, evict from mru
     *  Else evict from mfu
     */
    if (mru_size >= mru_target_size_) {
        // Try evicting from mru. If no evictables, try mfu.
        auto frameEvict = rfind_evictable(mru_, alive_map_);
        if (frameEvict != mru_.rend()) {
            // Can evict from mru
            // Move from mru to mru_ghost
            auto frameEvictIt   = std::next(frameEvict).base();
            frameId             = *frameEvictIt;
            auto frameStatus    = alive_map_[frameId];

            alive_map_.erase(frameId);

            frameStatus->arc_status_    = ArcStatus::MRU_GHOST;
            frameStatus->evictable_     = true;

            mru_ghost_.emplace_front(frameStatus->page_id_);
            mru_.erase(frameEvictIt);

            frameStatus->ghostIter      = mru_ghost_.begin();

            ghost_map_.insert({frameStatus->page_id_, frameStatus});
        } else {
            // Have to evict from mfu now
            auto frameEvictMfu = rfind_evictable(mfu_, alive_map_);
            if (frameEvictMfu == mfu_.rend()) {
                // This should not be possible since there should be atleast one evictable
                // if curr_size_ > 0
                throw Exception("Invalid state. No evictable frames found when curr_size_ > 0");
            }
            auto frameEvictIt   = std::next(frameEvictMfu).base();
            frameId             = *frameEvictIt;
            auto frameStatus    = alive_map_[frameId];

            alive_map_.erase(frameId);

            frameStatus->arc_status_    = ArcStatus::MFU_GHOST;
            frameStatus->evictable_     = true;

            mfu_ghost_.emplace_front(frameStatus->page_id_);
            mfu_.erase(frameEvictIt);

            frameStatus->ghostIter      = mfu_ghost_.begin();

            ghost_map_.insert({frameStatus->page_id_, frameStatus});
        }

    } else {
        // Try evicting from mfu. If not possible, try mru.
        auto frameEvict = rfind_evictable(mfu_, alive_map_);
        if (frameEvict != mfu_.rend()) {
            // Can evict from mfu
            // Move from mfu to mfu_ghost
            auto frameEvictIt   = std::next(frameEvict).base();
            frameId             = *frameEvictIt;
            auto frameStatus    = alive_map_[frameId];

            alive_map_.erase(frameId);

            frameStatus->arc_status_    = ArcStatus::MFU_GHOST;
            frameStatus->evictable_     = true;

            mfu_ghost_.emplace_front(frameStatus->page_id_);
            mfu_.erase(frameEvictIt);

            frameStatus->ghostIter      = mfu_ghost_.begin();

            ghost_map_.insert({frameStatus->page_id_, frameStatus});
        } else {
            // Have to evict from mru now
            auto frameEvictMru = rfind_evictable(mru_, alive_map_);
            if (frameEvictMru == mru_.rend()) {
                // This should not be possible since there should be atleast one evictable
                // if curr_size_ > 0
                throw Exception("Invalid state. No evictable frames found when curr_size_ > 0");
            }
            auto frameEvictIt   = std::next(frameEvictMru).base();
            frameId             = *frameEvictIt;
            auto frameStatus    = alive_map_[frameId];

            alive_map_.erase(frameId);

            frameStatus->arc_status_    = ArcStatus::MRU_GHOST;
            frameStatus->evictable_     = true;

            mru_ghost_.emplace_front(frameStatus->page_id_);
            mru_.erase(frameEvictIt);

            frameStatus->ghostIter      = mru_ghost_.begin();

            ghost_map_.insert({frameStatus->page_id_, frameStatus});
        }
    }

    // Adjust the count of evictables
    curr_size_--;
    return frameId;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 * by bring the accessed page to the front of mfu_ if it exists in any of the lists
 * or the front of mru_ if it does not.
 *
 * Performs the operations EXCEPT REPLACE described in original paper, which is
 * handled by `Evict()`.
 *
 * Consider the following four cases, handle accordingly:
 * 1. Access hits mru_ or mfu_
 * 2/3. Access hits mru_ghost_ / mfu_ghost_
 * 4. Access misses all the lists
 *
 * This routine performs all changes to the four lists as preperation
 * for `Evict()` to simply find and evict a victim into ghost lists.
 *
 * Note that frame_id is used as identifier for alive pages and
 * page_id is used as identifier for the ghost pages, since page_id is
 * the unique identifier to the page after it's dead.
 * Using page_id for alive pages should be the same since it's one to one mapping,
 * but using frame_id is slightly more intuitive.
 *
 * @param frame_id id of frame that received a new access.
 * @param page_id id of page that is mapped to the frame.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
    /**
     * If the page in alive map:
     *  If the page is in mru
     *  If the page is in mfu
     *  Query by frame_id
     * Else if page is in ghost map:
     *  If page is in ghost mru
     *  If page is in ghost mfu
     *  Query by page_id
     * Else (not in any list)
     *
     * Adjust mru_target_size as needed !!
     * Adjust curr_size_ as needed      !!
    */
    std::unique_lock writeLock(latch_);

    if (alive_map_.find(frame_id) != alive_map_.end()) {
        auto frameStatus = alive_map_[frame_id];
        switch (frameStatus->arc_status_)
        {
        case ArcStatus::MRU:
            {
                // Remove from MRU list
                auto iter = frameStatus->iter;
                mru_.erase(iter);

                frameStatus->arc_status_    = ArcStatus::MFU;

                // Add to MFU list
                mfu_.emplace_front(frame_id);
                frameStatus->iter = mfu_.begin();
            }
            break;
        case ArcStatus::MFU:
            {
                // TODO: Potential optimization when the page is already at the top
                // Remove from MFU list
                auto iter = frameStatus->iter;
                mfu_.erase(iter);

                frameStatus->arc_status_    = ArcStatus::MFU;

                // Add to MFU list
                mfu_.emplace_front(frame_id);
                frameStatus->iter = mfu_.begin();
            }
            break;
        default:
            throw Exception("Invalid frame status");
            break;
        }
    } else if (
        ghost_map_.find(page_id) != ghost_map_.end()
    ) {
        auto frameStatus = ghost_map_[page_id];
        switch (frameStatus->arc_status_)
        {
        case ArcStatus::MRU_GHOST:
            {
                // Adjust target size
                auto mfu_size = mfu_ghost_.size();
                auto mru_size = mru_ghost_.size();

                if (mru_size > mfu_size) {
                    mru_target_size_++;
                } else {
                    mru_target_size_ += (mfu_size / mru_size);
                }

                mru_target_size_ = std::min(mru_target_size_, replacer_size_);

                // Remove from mru ghost list and ghost map
                ghost_map_.erase(page_id);
                auto iter = frameStatus->ghostIter;
                mru_ghost_.erase(iter);

                frameStatus->arc_status_    = ArcStatus::MFU;

                // Add to mfu list and alive map
                mfu_.emplace_front(frame_id);
                frameStatus->iter = mfu_.begin();
                alive_map_.insert({frame_id, frameStatus});
            }
            break;
        case ArcStatus::MFU_GHOST:
            {
                // Adjust target size
                auto mfu_size = mfu_ghost_.size();
                auto mru_size = mru_ghost_.size();

                if (mfu_size > mru_size) {
                    mru_target_size_--;
                } else {
                    mru_target_size_ -= (mru_size / mfu_size);
                }

                mru_target_size_ = std::max(mru_target_size_, size_t(0));

                // Remove from mfu ghost list and ghost map
                ghost_map_.erase(page_id);
                auto iter = frameStatus->ghostIter;
                mfu_ghost_.erase(iter);

                frameStatus->arc_status_    = ArcStatus::MFU;

                // Add to mfu list and alive map
                mfu_.emplace_front(frame_id);
                frameStatus->iter = mfu_.begin();
                alive_map_.insert({frame_id, frameStatus});
            }
            break;
        default:
            throw Exception("Invalid frame status");
            break;
        }

        // Number of evictable pages increase
        curr_size_++;
    } else {
        // Case where page is not in any list
        auto mru_size       = mru_.size();
        auto mfu_size       = mfu_.size();
        auto mru_ghost_size = mru_ghost_.size();
        auto mfu_ghost_size = mfu_ghost_.size();

        if (
            mru_size + mru_ghost_size == replacer_size_ &&
            mru_ghost_size > 0
        ) {
            // Reuse frame status block from mru ghost list

            // Remove from mru ghost list and ghost map
            auto page_to_evict  = mru_ghost_.back();
            auto oldFrameStatus = ghost_map_[page_to_evict];

            ghost_map_.erase(page_to_evict);
            mru_ghost_.pop_back();

            // Repopulate the old frame status block
            oldFrameStatus->arc_status_     = ArcStatus::MRU;
            oldFrameStatus->evictable_      = true;
            oldFrameStatus->frame_id_       = frame_id;
            oldFrameStatus->page_id_        = page_id;

            // Add to mru list and the alive map
            mru_.emplace_front(frame_id);
            alive_map_.insert({frame_id, oldFrameStatus});
            oldFrameStatus->iter            = mru_.begin();

        } else if (
            mru_size + mfu_size + mru_ghost_size + mfu_ghost_size == 2 * replacer_size_ &&
            mfu_ghost_size > 0
        ) {
            // Reuse frame status block from mfu ghost list

            // Remove from mfu ghost list and ghost map
            auto page_to_evict  = mfu_ghost_.back();
            auto oldFrameStatus = ghost_map_[page_to_evict];

            ghost_map_.erase(page_to_evict);
            mfu_ghost_.pop_back();

            // Repopulate the old frame status block
            oldFrameStatus->arc_status_     = ArcStatus::MRU;
            oldFrameStatus->evictable_      = true;
            oldFrameStatus->frame_id_       = frame_id;
            oldFrameStatus->page_id_        = page_id;

            // Add to mru list and the alive map
            mru_.emplace_front(frame_id);
            alive_map_.insert({frame_id, oldFrameStatus});
            oldFrameStatus->iter            = mru_.begin();

        } else {
            // Create new frame status block
            auto newFrameStatus = std::make_shared<FrameStatus>(page_id, frame_id, true, ArcStatus::MRU);

            // Add to mru list and alive map
            mru_.emplace_front(frame_id);
            newFrameStatus->iter = mru_.begin();
            alive_map_.insert({frame_id, newFrameStatus});
        }

        // Evictable entries count increase
        curr_size_++;
    }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::unique_lock writeLock(latch_);

    if (alive_map_.find(frame_id) == alive_map_.end()) {
        throw bustub::Exception("Frame not found");
    }
    auto frameStatus = alive_map_[frame_id];
    if (set_evictable == frameStatus->evictable_) {
        return;
    }
    if (set_evictable){
        curr_size_++;
    } else {
        curr_size_--;
    }
    frameStatus->evictable_ = set_evictable;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
    std::unique_lock writeLock(latch_);

    if (alive_map_.find(frame_id) == alive_map_.end()) {
        return;
    }

    auto frameStatus = alive_map_[frame_id];
    if (!frameStatus->evictable_) {
        throw Exception("Do not evict a non-evictable frame");
    }


    auto arc_status = frameStatus->arc_status_;
    switch (arc_status)
    {
    case ArcStatus::MRU:
        {
            alive_map_.erase(frame_id);
            auto mru_it = std::find(mru_.begin(), mru_.end(), frame_id);
            if (mru_it == mru_.end()) {
                throw Exception("Frame not found");
            }
            mru_.erase(mru_it);

            frameStatus->arc_status_ = ArcStatus::MRU_GHOST;
            frameStatus->evictable_ = false;

            mru_ghost_.push_back(frame_id);
            ghost_map_.insert({frame_id, frameStatus});

            curr_size_--;
        } break;
    case ArcStatus::MFU:
        {
            alive_map_.erase(frame_id);
            auto mfu_it = std::find(mfu_.begin(), mfu_.end(), frame_id);
            if (mfu_it == mfu_.end()) {
                throw Exception("Frame not found");
            }
            mfu_.erase(mfu_it);

            frameStatus->arc_status_ = ArcStatus::MFU_GHOST;
            frameStatus->evictable_ = false;

            mfu_ghost_.push_back(frame_id);
            ghost_map_.insert({frame_id, frameStatus});

            curr_size_--;
        } break;
    default:
        throw Exception("Cannot remove frame thats not in cache");
        break;
    }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t {
    std::shared_lock readLock(latch_);
    return this->curr_size_;
}

}  // namespace bustub
