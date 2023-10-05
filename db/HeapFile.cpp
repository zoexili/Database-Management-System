#include <db/HeapFile.h>
#include <db/TupleDesc.h>
#include <db/Page.h>
#include <db/PageId.h>
#include <db/HeapPage.h>
#include <stdexcept>
#include <sys/stat.h>
#include <fcntl.h>

using namespace db;

//
// HeapFile
//

// TODO pa1.5: implement
HeapFile::HeapFile(const char *fname, const TupleDesc &td) {
    filename = fname;
    TD = td;
}

int HeapFile::getId() const {
    // TODO pa1.5: implement
    return std::hash<std::string>{}(filename);
}

const TupleDesc &HeapFile::getTupleDesc() const {
    // TODO pa1.5: implement
    return TD;
}

Page *HeapFile::readPage(const PageId &pid) {
    // TODO pa1.5: implement
    BufferPool &bp = Database::getBufferPool();
    int pgSize = bp.getPageSize();
    uint8_t *buffer = new uint8_t[pgSize];
    int offset = bp.getPageSize() * pid.pageNumber();

    std::ifstream file(filename, std::ios::binary);
    file.seekg(offset, std::ios::beg);
    file.read(reinterpret_cast<char*>(buffer), pgSize);
    file.close();
    const HeapPageId* heapPageIdPtr = dynamic_cast<const HeapPageId*>(&pid);
    HeapPage* page = new HeapPage(*heapPageIdPtr, buffer);
    return page;
}

int HeapFile::getNumPages() const {
    // TODO pa1.5: implement
    BufferPool &bp = Database::getBufferPool();
    // std::cout << "Filename: " << filename << std::endl;
    std::ifstream file(filename, std::ios::ate | std::ios::binary);  // open the file at the end
    long fileSize = file.tellg(); 
    file.close();
    return static_cast<int>(fileSize / bp.getPageSize());
}

HeapFileIterator HeapFile::begin() const {
    // TODO pa1.5: implement
    return HeapFileIterator(TID, 0, this);
}

HeapFileIterator HeapFile::end() const {
    // TODO pa1.5: implement
    return HeapFileIterator(TID, getNumPages(), this);
}

//
// HeapFileIterator
//

HeapFileIterator::HeapFileIterator() {
    pagePosition = 0;
    file = nullptr;
}

// TODO pa1.5: implement
HeapFileIterator::HeapFileIterator(TransactionId *tid, int pagePos, const HeapFile *hpFile) {
    // TODO pa1.5: implement
    TID = tid;
    pagePosition = pagePos;
    file = hpFile;
    
    if (pagePos < file->getNumPages()) {
        HeapPageId pageId(hpFile->getId(), pagePos); 
        HeapPage* currPage = static_cast<HeapPage*>(Database::getBufferPool().getPage(*TID, &pageId));
        currPageIterator = currPage->begin();
    }
}

// compare two iterators
bool HeapFileIterator::operator!=(const HeapFileIterator &other) const {
    // TODO pa1.5: implement
    return pagePosition != other.pagePosition || file != other.file;
}

// use its current position to get tuples from the current page.
Tuple &HeapFileIterator::operator*() const {
    // TODO pa1.5: implement
    return *currPageIterator;
}

// iterator go forward
HeapFileIterator &HeapFileIterator::operator++() {
    // TODO pa1.5: implement
    HeapPageId pageId(file->getId(), pagePosition); 
    HeapPage* currPage = static_cast<HeapPage*>(Database::getBufferPool().getPage(*TID, &pageId));
    
    if (currPageIterator != currPage->end()) {
        ++currPageIterator;
    } 
    
    // when we hit the end of page, go to next page. 
    // I added a == operator to make sure page always switches when hits the end.
    while (currPageIterator == currPage->end()) {
        ++pagePosition;
        if (pagePosition >= file->getNumPages()) {
            // if no more pages, return
            return *this;
        }
        
        pageId = HeapPageId(file->getId(), pagePosition); 
        currPage = dynamic_cast<HeapPage*>(Database::getBufferPool().getPage(*TID, &pageId));
        currPageIterator = currPage->begin();
    }
    
    return *this;
}
