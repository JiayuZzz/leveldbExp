//
// Created by wujy on 11/3/18.
//

#ifndef LEVELDB_GCTABLE_H
#define LEVELDB_GCTABLE_H

#include <map>
#include <set>
#include "iostream"
#include <vector>

namespace leveldb {
    struct Gcnode {
        int vlogNum;
        int gSize;
        std::set<int> offsets;
        Gcnode* next;
        Gcnode* pre;

        Gcnode(int num, Gcnode* p, Gcnode* nex = nullptr):vlogNum(num),gSize(0),offsets(std::set<int>()),pre(p),next(nex){}
    };

    //TODO: persist on disk
    class Gctable {
    private:
        std::map<int, Gcnode*> numToNode;
        Gcnode* first;
        Gcnode* last;
        Gcnode* toDel;   // previous last node

        Gcnode* Insert(int vlogNum){
            Gcnode* newNode = new Gcnode(vlogNum,nullptr,first);
            if(first) first->pre = newNode;
            else last = newNode;
            first = newNode;
            numToNode[vlogNum] = newNode;
            return newNode;
        }

    public:
        Gctable():numToNode(std::map<int, Gcnode*>()),first(nullptr),last(nullptr){}

        ~Gctable(){
            if(toDel) delete toDel;
            while(first){
                Gcnode* tmp = first;
                first = first->next;
                delete tmp;
            }
        }

        Gcnode* GetLast(){
            return last;
        }

        Gcnode* GetNode(int vlogNum){
//            if(last==nullptr) std::cerr<<"n\n";
//            else std::cerr<<last->gSize<<"\n";
            auto iter = numToNode.find(vlogNum);
            if(iter == numToNode.end()) return nullptr;
            else return iter->second;
        }

        void DropLast(){
            if(last!=nullptr){
                numToNode.erase(last->vlogNum);
                // delete invalid node next time
                if(toDel){
                    delete toDel;
                }
                toDel = last;
                last = last->pre;
//                if(!last) first = nullptr;
                // remove invalid node until add
                if(last) last->next = nullptr;
                else first = nullptr;
            }
        }

        void Add(int vlogNum, int size, const std::vector<int>& offsets){
            // delete invalid nodes
//            if(last&&last->next!=nullptr){
//                Gcnode* tmp = last->next;
//                last-> next = nullptr;
//                while(tmp){
//                    Gcnode* tmp1 = tmp;
//                    tmp = tmp->next;
//                    numToNode.erase(tmp1->vlogNum);
//                    delete tmp1;
//                }
//            }
            Gcnode* n = GetNode(vlogNum);
            if(n==nullptr){
                n = Insert(vlogNum);
            }
//            std::cout<<n->gSize<<" "<<size<<std::endl;
            n->gSize+=size;
            for(auto &offset : offsets){
                n->offsets.insert(offset);
            }
            // sort
            Gcnode* ptr = n;
            while(ptr->next&&ptr->next->gSize<n->gSize){
                ptr = ptr->next;
            }
            if(ptr!=n){
                if(ptr->next) ptr->next->pre = n;
                n->next->pre = n->pre;
                if(n->pre) n->pre->next = n->next;
                else first = n->next;
                n->next = ptr->next;
                n->pre = ptr;
                ptr->next = n;
            }
            if(n->next==nullptr){
                last = n;
            }
        }

        bool IsEmpty(){
            return last == nullptr;
        }
    };
}

#endif //LEVELDB_GCTABLE_H
