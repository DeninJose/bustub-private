//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <stdexcept>
#include <string>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  /** @TODO(student) Implement this function! */
  if(width * depth == 0){
    throw std::invalid_argument("Size cannot be zero");
  }
  sketchTable.resize(width_ * depth_, 0);

  /** @fall2025 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept : width_(other.width_), depth_(other.depth_) {
  /** @TODO(student) Implement this function! */
  width_ = other.width_;
  depth_ = other.depth_;
  hash_functions_ = std::move(other.hash_functions_);
  sketchTable = std::move(other.sketchTable);
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */
  if(this == &other) return *this;
  width_ = other.width_;
  other.width_ = 0;

  depth_ = other.depth_;
  other.depth_ = 0;

  hash_functions_ = std::move(other.hash_functions_);
  sketchTable = std::move(other.sketchTable);

  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  /** @TODO(student) Implement this function! */
  for(size_t row = 0; row < depth_; row++){
    auto& hash = hash_functions_[row];
    auto col = hash(item);

    std::lock_guard<std::mutex> lock(sketchMutex);
    sketchTable[row * width_ + col]++;
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  /** @TODO(student) Implement this function! */
  for(size_t row = 0; row < depth_; row++){
    for(size_t col = 0; col < width_; col++){
      this->sketchTable[row * width_ + col] += other.sketchTable[row * width_ + col];
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  uint32_t minm = std::numeric_limits<uint32_t>::max();
  for(size_t row = 0; row < depth_; row++){
    size_t col = (hash_functions_[row](item) % width_);
    // std::cout << row << " " << col << " " << sketchTable.size() << std::endl;
    minm = std::min(minm, sketchTable[row * width_ + col]);
  }
  return minm;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  std::fill(sketchTable.begin(), sketchTable.end(), 0);
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */
  typedef std::pair<KeyType, uint32_t> pair;
  std::vector<pair> pairs;
  pairs.reserve(candidates.size());

  for(auto& candidate : candidates){
    pairs.push_back({candidate, Count(candidate)});
  }

  std::sort(pairs.begin(), pairs.end(), [](auto a, auto b){ return a.second > b.second; });
  pairs.resize(k);
  return pairs;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
