#include <db/Catalog.h>
#include <db/TupleDesc.h>
#include <stdexcept>

using namespace db;

void Catalog::addTable(DbFile *file, const std::string &name, const std::string &pkeyField) {
    // TODO pa1.2: implement
    int tableID = file->getId();

    for (auto &key: names) {
        if (key.second == name) {
            if (key.first != tableID) {
                names.erase(key.first);
                dbFiles.erase(key.first);
                pkeyFields.erase(key.first);
            }
            break;
        }
    }
    names.insert({tableID, name});
    dbFiles.insert({tableID, file});
    pkeyFields.insert({tableID, pkeyField});
}

int Catalog::getTableId(const std::string &name) const {
    // TODO pa1.2: implement
    for (auto &key: names) {
        if (key.second == name) {
            return key.first;
        }
    }
    throw std::invalid_argument("This table name does not exist");
}

const TupleDesc &Catalog::getTupleDesc(int tableid) const {
    // TODO pa1.2: implement
    if (dbFiles.count(tableid) > 0) {
        return dbFiles.find(tableid)->second->getTupleDesc();
    }
}

DbFile *Catalog::getDatabaseFile(int tableid) const {
    // TODO pa1.2: implement
    return dbFiles.find(tableid)->second;
}

std::string Catalog::getPrimaryKey(int tableid) const {
    // TODO pa1.2: implement
    return pkeyFields.find(tableid)->second;
}

std::string Catalog::getTableName(int id) const {
    // TODO pa1.2: implement
    return names.find(id)->second;
}

void Catalog::clear() {
    // TODO pa1.2: implement
    names.clear();
    dbFiles.clear();
    pkeyFields.clear();
}
