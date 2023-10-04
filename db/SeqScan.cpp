#include <db/SeqScan.h>

using namespace db;

// TODO pa1.6: implement
// 在构造函数中初始化引用TABLE_ALIAS
SeqScan::SeqScan(TransactionId* tid, int tableid, const std::string &tableAlias) {
    this->TID = tid;
    this->TABLE_ID = tableid;
    this->TABLE_ALIAS = tableAlias;
}

std::string SeqScan::getTableName() const {
    // TODO pa1.6: implement
    return Database::getCatalog().getTableName(TABLE_ID);
}

std::string SeqScan::getAlias() const {
    // TODO pa1.6: implement
    return TABLE_ALIAS;
}

void SeqScan::reset(int tabid, const std::string &tableAlias) {
    // TODO pa1.6: implement
    this->TABLE_ID = tabid;
    this->TABLE_ALIAS = tableAlias;
}

const TupleDesc &SeqScan::getTupleDesc() const {
    // TODO pa1.6: implement
    return Database::getCatalog().getTupleDesc(TABLE_ID);
}

SeqScan::iterator SeqScan::begin() const {
    // TODO pa1.6: implement
    HeapFile *currFile = static_cast<HeapFile*>(Database::getCatalog().getDatabaseFile(TABLE_ID));
    return SeqScanIterator(TID, currFile->begin());
}

SeqScan::iterator SeqScan::end() const {
    // TODO pa1.6: implement
    HeapFile *currFile = static_cast<HeapFile*>(Database::getCatalog().getDatabaseFile(TABLE_ID));
    return SeqScanIterator(TID, currFile->end());
}

//
// SeqScanIterator
//

// TODO pa1.6: implement
SeqScanIterator::SeqScanIterator(TransactionId *tid, HeapFileIterator currFileIterator):TID(tid), currFileIterator(currFileIterator) {
    // this->TID = tid;
    // HeapFile *currFile = static_cast<HeapFile*>(Database::getCatalog().getDatabaseFile(tableid));
    // currFileIterator = HeapFileIterator(tid, 0, currFile);
}

bool SeqScanIterator::operator!=(const SeqScanIterator &other) const {
    // TODO pa1.6: implement
    return TID != other.TID || currFileIterator != other.currFileIterator;
}

SeqScanIterator &SeqScanIterator::operator++() {
    // TODO pa1.6: implement
    ++currFileIterator;
    return *this;
}

const Tuple &SeqScanIterator::operator*() const {
    // TODO pa1.6: implement
    return *currFileIterator;
}
