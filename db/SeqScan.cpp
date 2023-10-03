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
    return SeqScanIterator(TID, TABLE_ID, false);
}

SeqScan::iterator SeqScan::end() const {
    // TODO pa1.6: implement
    return SeqScanIterator(TID, TABLE_ID, true);
}

//
// SeqScanIterator
//

// TODO pa1.6: implement
// isEnd indicates if the iterator reaches to the end.
SeqScanIterator::SeqScanIterator(TransactionId *tid, int tableid, bool isEnd) {
    this->TID = tid;
    this->TABLE_ID = tableid;
    this->IS_End = isEnd;
    if (!isEnd) {
        HeapFile *currFile = static_cast<HeapFile*>(Database::getCatalog().getDatabaseFile(tableid));
        currFileIterator = HeapFileIterator(tid, 0, currFile);
    }   
}

bool SeqScanIterator::operator!=(const SeqScanIterator &other) const {
    // TODO pa1.6: implement
    if (IS_End && other.IS_End) {
        return false;
    }
    if (IS_End || other.IS_End) {
        return true;
    }
    return TID != other.TID || TABLE_ID != other.TABLE_ID || currFileIterator != other.currFileIterator;
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
