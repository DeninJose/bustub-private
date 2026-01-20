//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <vector>
#include "common/macros.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param requests The requests to be scheduled.
 */
void DiskScheduler::Schedule(std::vector<DiskRequest> &requests) {
  // Add all the requests into the channel
  std::for_each(requests.begin(), requests.end(), [this](auto& request){
    request_queue_.Put(std::make_optional<DiskRequest>(std::move(request)));
  });
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
 * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
 */
void DiskScheduler::StartWorkerThread() {
  /**
   * while there are requests in the queue,
   * keep processing them
   * If a null optional is found,
   * break out from the processing loop
   */
  std::optional<DiskRequest> diskRequest = request_queue_.Get();

  while (diskRequest.has_value()) {
    auto& request = diskRequest.value();

    if (request.is_write_) {
      disk_manager_->WritePage(request.page_id_, request.data_);
    } else {
      disk_manager_->ReadPage(request.page_id_, request.data_);
    }
    request.callback_.set_value(true);

    // Get next request
    diskRequest = request_queue_.Get();
  }
}

}  // namespace bustub
