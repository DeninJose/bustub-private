//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <memory>
#include "buffer/arc_replacer.h"
#include "common/macros.h"

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to read.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id,
                             std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<ArcReplacer> replacer,
                             std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  /**
   * Increment the pin count
   * Sync with arc replacer
   */
  frame_      ->  pin_count_++;
  replacer_   ->  SetEvictable(frame_->frame_id_, false);
  read_lock_  =   std::make_unique<std::shared_lock<std::shared_mutex>>(frame_->rwlatch_);
  is_valid_   =   true;
}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  // Transfer resources
  this->bpm_latch_      = std::move(that.bpm_latch_);
  this->disk_scheduler_ = std::move(that.disk_scheduler_);
  this->replacer_       = std::move(that.replacer_);
  this->frame_          = std::move(that.frame_);
  this->read_lock_      = std::move(that.read_lock_);
  this->page_id_        = that.page_id_;
  this->is_valid_       = that.is_valid_;

  // Invalidate the moved object
  that.is_valid_        = false;
}

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  ReadPageGuard copy(std::move(that));
  swap(copy, *this);
  return *this;
}

auto ReadPageGuard::swap(ReadPageGuard& a, ReadPageGuard& b) -> void {
  std::swap(a.page_id_,         b.page_id_);
  std::swap(a.frame_,           b.frame_);
  std::swap(a.replacer_,        b.replacer_);
  std::swap(a.bpm_latch_,       b.bpm_latch_);
  std::swap(a.disk_scheduler_,  b.disk_scheduler_);
  std::swap(a.read_lock_,       b.read_lock_);
  std::swap(a.is_valid_,        b.is_valid_);
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Flush() {
  /**
   * If page is not dirty, return
   * Else
   *
   * Mark the page as not dirty
   * Flush through the disk
   */
  if (!frame_->is_dirty_) return;

  std::vector<DiskRequest> requests(1);
  auto& request = requests[0];

  request.is_write_   = true;
  request.page_id_    = page_id_;
  request.data_       = frame_->GetDataMut();

  auto future = request.callback_.get_future();
  disk_scheduler_->Schedule(requests);
  future.get(); // What to do with this ?

  // Only now can the frame be marked clean
  frame_->is_dirty_ = false;
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Drop() {
  /**
   * First release the lock on the frame to prevent holding multiple locks at the same time
   * Acquire the buffer pool manager lock
   * Unpin the page from the frame
   * Sync with arc replacer if the frame is now evictable
   */

  if (!is_valid_) return;
  is_valid_ = false;

  read_lock_.reset();
  // Lock on the buffer pool since we are signalling the replacer
  // Lock on the frame since we are updating its pin count
  std::scoped_lock bpmScopedLock(*bpm_latch_);

  frame_->pin_count_--;
  if (frame_->pin_count_ == 0) {
    // Let the arc replacer know that the page is now evictable
    replacer_->SetEvictable(frame_->frame_id_, true);
  }
}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  frame_           ->  pin_count_++;
  replacer_        ->  SetEvictable(frame_->frame_id_, false);
  write_lock_       =  std::make_unique<std::unique_lock<std::shared_mutex>>(frame_->rwlatch_);
  is_valid_         =  true;
  frame_->is_dirty_ =  true;
}

/**
 * @brief The move constructor for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  // Transfer resources
  this->bpm_latch_      = std::move(that.bpm_latch_);
  this->disk_scheduler_ = std::move(that.disk_scheduler_);
  this->replacer_       = std::move(that.replacer_);
  this->frame_          = std::move(that.frame_);
  this->write_lock_     = std::move(that.write_lock_);
  this->page_id_        = that.page_id_;
  this->is_valid_       = that.is_valid_;

  // Invalidate the moved object
  that.is_valid_        = false;
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return WritePageGuard& The newly valid `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  WritePageGuard copy(std::move(that));
  swap(copy, *this);
  return *this;
}

auto WritePageGuard::swap(WritePageGuard& a, WritePageGuard& b) -> void {
  std::swap(a.page_id_,         b.page_id_);
  std::swap(a.frame_,           b.frame_);
  std::swap(a.replacer_,        b.replacer_);
  std::swap(a.bpm_latch_,       b.bpm_latch_);
  std::swap(a.disk_scheduler_,  b.disk_scheduler_);
  std::swap(a.write_lock_,      b.write_lock_);
  std::swap(a.is_valid_,        b.is_valid_);
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Flush() {
  /**
   * If page is not dirty, return
   * Else
   *
   * Mark the page as not dirty
   * Flush through the disk
   */
  if (!frame_->is_dirty_) return;

  std::vector<DiskRequest> requests(1);
  auto& request = requests[0];

  request.is_write_   = true;
  request.page_id_    = page_id_;
  request.data_       = frame_->GetDataMut();

  auto future = request.callback_.get_future();
  disk_scheduler_->Schedule(requests);
  future.get(); // What to do with this ?

  // Only now can the frame be marked clean
  frame_->is_dirty_ = false;
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Drop() {
  /**
   * First release the lock on the frame to prevent holding multiple locks at the same time
   * Acquire the buffer pool manager lock
   * Unpin the page from the frame
   * Sync with arc replacer if the frame is now evictable
   */

  if (!is_valid_) return;
  is_valid_ = false;

  write_lock_.reset();
  // Lock on the buffer pool since we are signalling the replacer
  // Lock on the frame since we are updating its pin count
  std::scoped_lock bpmScopedLock(*bpm_latch_);

  frame_->pin_count_--;
  if (frame_->pin_count_ == 0) {
    // Let the arc replacer know that the page is now evictable
    replacer_->SetEvictable(frame_->frame_id_, true);
  }
}

/** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub


/**
 * TODO LIST:
 *  * Maybe refactor common code into common class
 */