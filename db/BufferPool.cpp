#include <db/BufferPool.h>
#include <db/Database.h>

using namespace db;

// TODO pa1.3: implement
BufferPool::BufferPool(int numPages) {
    maxPages = numPages;
    pool.reserve(numPages);
}

Page *BufferPool::getPage(const TransactionId &tid, PageId *pid) {
    // TODO pa1.3: implement
    if (pool.count(*pid) > 0) {
        return &pool[*pid];
    }
    else {
        Catalog &c = db::Database::getCatalog();
        DbFile* dbfile = c.getDatabaseFile(pid->getTableId());
        Page* page = dbfile->readPage(*pid);
        pool[*pid] = *page;
    }
    return &pool[*pid];
}

void BufferPool::evictPage() {
    // do nothing for now
}
