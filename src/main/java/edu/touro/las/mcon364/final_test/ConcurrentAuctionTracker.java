package edu.touro.las.mcon364.final_test;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Concurrent Auction Tracker (ConcurrentSkipListSet + ExecutorService)
 *
 * Scenario: an online auction platform receives bid submissions from many bidders
 * at the same time. The tracker must always reflect the current top bids in
 * sorted order (highest first) and must be safe when read and written concurrently.
 *
 *  Requirements:
 * - submitBid(entry) adds a BidEntry thread-safely and counts the submission.
 * - getTopN(n) returns the top n BidEntry objects as an immutable list, highest first.
 * - getTotalBids() returns the number of times submitBid has been called.
 * - runSimulation(bidders, bidsEach) uses an ExecutorService to have each bidder
 *   submit bidsEach random bids concurrently, then shuts down the pool and waits.
 *
 * Do not use synchronized blocks. Rely on concurrent collections and atomic variables to ensure thread safety.
 */
public class ConcurrentAuctionTracker {

    //Uncomment line below and choose the appropriate concurrent collection to store BidEntry objects sorted by amount.
    private final ConcurrentSkipListSet<BidEntry> bids = new ConcurrentSkipListSet<>();


    // Initialize a thread-safe counter to track total bid submissions and call it totalBids.
    private final AtomicInteger totalBids = new AtomicInteger(0);


    /**
     * Adds a bid entry to the tracker thread-safely and increments the counter.
     *
     * @param entry the bid entry to add
     */
    public void submitBid(BidEntry entry) {
        // implement this method
        bids.add(entry);
        totalBids.incrementAndGet();
    }

    /**
     * Returns the top n bids as an immutable list, highest amount first.
     *
     * @param n number of top entries to return
     * @return immutable top-n list
     */
    public List<BidEntry> getTopN(int n) {
        // implement this method
        return bids.stream()
                .limit(n)
                .toList();
    }

    /**
     * Returns how many times submitBid has been called since creation.
     */
    public int getTotalBids() {
        // implement this method
        return totalBids.get();
    }

    /**
     * Simulates concurrent bid submissions using an ExecutorService.
     *
     * Each bidder in the list submits bidsEach random bids on a separate thread.
     * Wait for all threads to finish before returning.
     *
     * @param bidders   list of bidder identifiers
     * @param bidsEach  number of random bids each bidder submits
     */
    public void runSimulation(List<String> bidders, int bidsEach) throws InterruptedException {
// not sure if this is technically the way to build it, but it worked for me
        ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        bidders.stream().forEach((player) -> pool.submit(() -> IntStream.range(0, bidsEach)
                .forEach((i) -> this.submitBid(new BidEntry(player, (new Random()).nextInt(1000), System.currentTimeMillis())))));
        pool.shutdown();
        pool.awaitTermination(30L, TimeUnit.SECONDS);
    }
}

