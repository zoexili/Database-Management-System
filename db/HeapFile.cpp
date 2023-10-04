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

    if (pagePosition < file->getNumPages()) {
        HeapPageId pageId(file->getId(), pagePosition); 
        HeapPage* currPage = static_cast<HeapPage*>(Database::getBufferPool().getPage(*TID, &pageId));
        currPageIterator = currPage->begin();
    }
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

// 比较两个迭代器对象是否不同
bool HeapFileIterator::operator!=(const HeapFileIterator &other) const {
    // TODO pa1.5: implement
    return pagePosition != other.pagePosition || file != other.file;
}

// use its current position to get tuples from the current page.
Tuple &HeapFileIterator::operator*() const {
    // TODO pa1.5: implement
    return *currPageIterator;
}

// 使迭代器前进到下一个元组
HeapFileIterator &HeapFileIterator::operator++() {
    // TODO pa1.5: implement
    HeapPageId pageId(file->getId(), pagePosition); 
    HeapPage* currPage = static_cast<HeapPage*>(Database::getBufferPool().getPage(*TID, &pageId));
    if(currPageIterator != currPage->end()) {
        ++currPageIterator;
    } else {
        ++pagePosition;
        if (pagePosition < file->getNumPages()) {
            HeapPageId pageId(file->getId(), pagePosition); 
            HeapPage *currPage = dynamic_cast<HeapPage*>(Database::getBufferPool().getPage(*TID, &pageId));
            currPageIterator = currPage->begin();
        }
    }
    return *this;
}
