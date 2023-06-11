from IPython.display import clear_output
from pathlib import Path
from typing import List, Set, Union
from abc import abstractmethod
from functools import total_ordering
from os import path
import os
import pickle
import gc


class Index:
    def __init__(self):
        self.dic_index = {}
        self.set_documents = set()

    def index(self, term: str, doc_id: int, term_freq: int):
        if term not in self.dic_index:
            int_term_id = len(self.dic_index) + 1 #contagem é de 0 a n-1 ou de 1 a n
            self.dic_index[term] = self.create_index_entry(int_term_id)
        else:
            int_term_id = self.get_term_id(term)

        self.add_index_occur(self.dic_index[term], doc_id, int_term_id, term_freq)
        self.set_documents.add(doc_id)

    @property
    def vocabulary(self) -> List[str]:
        return self.dic_index.keys()

    @property
    def document_count(self) -> int:
        return len(self.set_documents)

    @abstractmethod
    def get_term_id(self, term: str):
        raise NotImplementedError("Voce deve criar uma subclasse e a mesma deve sobrepor este método")

    @abstractmethod
    def create_index_entry(self, termo_id: int):
        raise NotImplementedError("Voce deve criar uma subclasse e a mesma deve sobrepor este método")

    @abstractmethod
    def add_index_occur(self, entry_dic_index, doc_id: int, term_id: int, freq_termo: int):
        raise NotImplementedError("Voce deve criar uma subclasse e a mesma deve sobrepor este método")

    @abstractmethod
    def get_occurrence_list(self, term: str) -> List:
        raise NotImplementedError("Voce deve criar uma subclasse e a mesma deve sobrepor este método")

    @abstractmethod
    def document_count_with_term(self, term: str) -> int:
        raise NotImplementedError("Voce deve criar uma subclasse e a mesma deve sobrepor este método")

    def finish_indexing(self):
        pass

    def write(self, arq_index: str):
        file = open(arq_index, 'wb')
        pickle.dump(self, file)
        file.close()
    

    @staticmethod
    def read(arq_index: str):
        file = open(arq_index, 'rb')
        index = pickle.load(file)
        file.close()
        control = 0

        print(index.dic_index)
        for x in index.dic_index.keys():
            if type(index.dic_index[x]) is TermFilePosition:
                control = 2
            break

        if control == 0:
            for i in index.dic_index.values():
                for j in i:
                    index.set_documents.add(j.doc_id)
                    
        elif control == 2:
            for i in index.dic_index.values():
                index.set_documents.add(i.doc_count_with_term)
        return index

    def __str__(self):
        arr_index = []
        for str_term in self.vocabulary:
            arr_index.append(f"{str_term} -> {self.get_occurrence_list(str_term)}")

        return "\n".join(arr_index)

    def __repr__(self):
        return str(self)


@total_ordering
class TermOccurrence:
    def __init__(self, doc_id: int, term_id: int, term_freq: int):
        self.doc_id = doc_id
        self.term_id = term_id
        self.term_freq = term_freq

    def write(self, idx_file):
        idx_file.write(self.doc_id.to_bytes(4,byteorder="big"))
        idx_file.write(self.term_id.to_bytes(4,byteorder="big"))
        idx_file.write(self.term_freq.to_bytes(4,byteorder="big"))

    def __hash__(self):
        return hash((self.doc_id, self.term_id))

    def __eq__(self, other_occurrence: "TermOccurrence"):
        if other_occurrence is None:
            return False
        return (self.doc_id == other_occurrence.doc_id and self.term_id == other_occurrence.term_id)

    def __lt__(self, other_occurrence: "TermOccurrence"):
        if other_occurrence is None:
            return False
        else:
            if self.term_id < other_occurrence.term_id:
                return True
            elif self.term_id == other_occurrence.term_id:
                if self.doc_id < other_occurrence.doc_id:
                    return True
        return False
    
    def __gt__(self, other_occurrence: "TermOccurrence"):
        if other_occurrence is None:
            return False
        else:
            if self.term_id > other_occurrence.term_id:
                return True
            elif self.term_id == other_occurrence.term_id:
                if self.doc_id > other_occurrence.doc_id:
                    return True
        return False

    def __str__(self):
        return f"( doc: {self.doc_id} term_id:{self.term_id} freq: {self.term_freq})"

    def __repr__(self):
        return str(self)


# HashIndex é subclasse de Index
class HashIndex(Index):
    def get_term_id(self, term: str):
        return self.dic_index[term][0].term_id

    def create_index_entry(self, termo_id: int) -> List:
        return []

    def add_index_occur(self, entry_dic_index: List[TermOccurrence], doc_id: int, term_id: int, term_freq: int):
        entry_dic_index.append(TermOccurrence(doc_id, term_id, term_freq))

    def get_occurrence_list(self, term: str) -> List:
        if term not in self.dic_index:
            return []
        return self.dic_index[term]

    def document_count_with_term(self, term: str) -> int:
        if term not in self.dic_index:
            return 0
        return len(self.dic_index[term])
    
    def finish_indexing(self):
        self.write('HashWiki.bot')


class TermFilePosition:
    def __init__(self, term_id: int, term_file_start_pos: int = None, doc_count_with_term: int = None):
        self.term_id = term_id

        # a serem definidos após a indexação
        self.term_file_start_pos = term_file_start_pos
        self.doc_count_with_term = doc_count_with_term

    def __str__(self):
        return f"term_id: {self.term_id}, doc_count_with_term: {self.doc_count_with_term}, term_file_start_pos: {self.term_file_start_pos}"

    def __repr__(self):
        return str(self)


class FileIndex(Index):
    TMP_OCCURRENCES_LIMIT = 100000

    def __init__(self):
        super().__init__()

        self.lst_occurrences_tmp = [None]*FileIndex.TMP_OCCURRENCES_LIMIT
        self.idx_file_counter = 0
        self.str_idx_file_name = "occur_idx_"

        # metodos auxiliares para verifica o tamanho da lst_occurrences_tmp
        self.idx_tmp_occur_last_element  = -1
        self.idx_tmp_occur_first_element = 0
        
    def get_tmp_occur_size(self):
        """Retorna o tamanho da lista temporária de ocorrências"""
        return self.idx_tmp_occur_last_element - self.idx_tmp_occur_first_element + 1

    def get_term_id(self, term: str):
        return self.dic_index[term].term_id

    def create_index_entry(self, term_id: int) -> TermFilePosition:
        return TermFilePosition(term_id)

    def add_index_occur(self, entry_dic_index: TermFilePosition, doc_id: int, term_id: int, term_freq: int):
        #complete aqui adicionando um novo TermOccurrence na lista lst_occurrences_tmp
        #não esqueça de atualizar a(s) variável(is) auxiliares apropriadamente
        if self.lst_occurrences_tmp[99999] is not None:
            self.save_tmp_occurrences()
        else:
            self.idx_tmp_occur_last_element = self.idx_tmp_occur_last_element + 1
            self.lst_occurrences_tmp[self.idx_tmp_occur_last_element] = TermOccurrence(doc_id, term_id, term_freq)

    def next_from_list(self) -> TermOccurrence:
        if self.get_tmp_occur_size() > 0:
            # obtenha o proximo da lista e armazene em nex_occur
            # não esqueça de atualizar a(s) variável(is) auxiliares apropriadamente
            next_occur = self.lst_occurrences_tmp[self.idx_tmp_occur_first_element]
            del self.lst_occurrences_tmp[self.idx_tmp_occur_first_element]
            self.idx_tmp_occur_last_element = self.idx_tmp_occur_last_element - 1
            return next_occur
        else:
            return None

    def next_from_file(self, file_pointer) -> TermOccurrence:
        # next_from_file = pickle.load(file_idx)
        bytes_doc_id = file_pointer.read(4)
        if not bytes_doc_id:
            return None
            # seu código aqui :)
        doc_id = int.from_bytes(bytes_doc_id, byteorder='big')
        term_id = int.from_bytes(file_pointer.read(4),byteorder='big')
        if not term_id:
            return None
        term_freq = int.from_bytes(file_pointer.read(4),byteorder='big')
        if not term_freq:
            return None
        return TermOccurrence(doc_id, term_id, term_freq)

    def save_tmp_occurrences(self):

        # Ordena pelo term_id, doc_id
        #    Para eficiência, todo o código deve ser feito com o garbage collector desabilitado gc.disable()
        gc.disable()

        """comparar sempre a primeira posição
        da lista com a primeira posição do arquivo usando os métodos next_from_list e next_from_file
        e use o método write do TermOccurrence para armazenar cada ocorrencia do novo índice ordenado"""

        self.lst_occurrences_tmp = list(filter(lambda x: x is not None, self.lst_occurrences_tmp))

        for i in range(len(self.lst_occurrences_tmp)):
            for j in range(len(self.lst_occurrences_tmp)):
                if self.lst_occurrences_tmp[i] < self.lst_occurrences_tmp[j]:
                    temp = self.lst_occurrences_tmp[i]
                    self.lst_occurrences_tmp[i] = self.lst_occurrences_tmp[j]
                    self.lst_occurrences_tmp[j] = temp


        file_name_old = 'occur_idx_' + str(self.idx_file_counter) + '.idx'
        #self.idx_file_counter = self.idx_file_counter + 1
        file_name_new = 'occur_idx_' + str(self.idx_file_counter+1) + '.idx'
        if os.path.exists(file_name_old) == False:
            arq_new = open(file_name_old, "wb")
            self.str_idx_file_name = file_name_old
            list_term = self.next_from_list()
            while(list_term is not None):
                list_term.write(arq_new)
                list_term = self.next_from_list()
            
        else:
            self.idx_file_counter = self.idx_file_counter + 1
            arq_old = open(file_name_old, "rb")
            arq_new = open(file_name_new, "wb")
            self.str_idx_file_name = file_name_new
            list_term = self.next_from_list()
            file_term = self.next_from_file(arq_old)
            if list_term is not None and file_term is not None:
                while(file_term is not None):
                    if list_term is None:
                        file_term.write(arq_new)
                        file_term = self.next_from_file(arq_old)
                    elif list_term < file_term:
                        list_term.write(arq_new)
                        list_term = self.next_from_list()
                    elif list_term > file_term:
                        file_term.write(arq_new)
                        file_term = self.next_from_file(arq_old)
                    else:
                        list_term.write(arq_new)
                        list_term = self.next_from_list()
                        file_term = self.next_from_file(arq_old)
            
            arq_old.close()
            arq_new.close()
            os.remove(file_name_old)


        gc.enable()

    def finish_indexing(self):
        if len(self.lst_occurrences_tmp) > 0:
            self.save_tmp_occurrences()
            self.lst_occurrences_tmp = [None]*FileIndex.TMP_OCCURRENCES_LIMIT
            self.idx_file_counter = 0
            self.idx_tmp_occur_last_element  = -1
            self.idx_tmp_occur_first_element = 0

        # Sugestão: faça a navegação e obetenha um mapeamento
        # id_termo -> obj_termo armazene-o em dic_ids_por_termo
        dic_ids_por_termo = {}
        for str_term, obj_term in self.dic_index.items():
            dic_ids_por_termo[obj_term.term_id] = str_term

        with open(self.str_idx_file_name, 'rb') as idx_file:
            # navega nas ocorrencias para atualizar cada termo em dic_ids_por_termo
            # apropriadamente

            idx_file.read(4)
            previous_term_id = int.from_bytes(
                idx_file.read(4), byteorder='big')
            term_file_start_pos = 0
            cont_n_docs = 1
            cont = 0

            tam_term_occurrance = 12

            while previous_term_id:
                idx_file.read(8)
                term_id = int.from_bytes(idx_file.read(4), byteorder='big')

                if term_id == previous_term_id:
                    cont_n_docs += 1
                else:
                    start_pos = term_file_start_pos - \
                        (cont_n_docs - 1)*tam_term_occurrance
                    t = TermFilePosition(
                        previous_term_id, start_pos, cont_n_docs)
                    cont_n_docs = 1
                    key_dic = dic_ids_por_termo[previous_term_id]
                    self.dic_index[key_dic] = t

                term_file_start_pos += tam_term_occurrance

                # atualiza o anterior
                previous_term_id = term_id

    def get_occurrence_list(self, term: str) -> List:
        if term not in self.dic_index:
            return []
        occur_list = []
        position_in_file = self.dic_index[term].term_file_start_pos
        num_of_occur = self.dic_index[term].doc_count_with_term

        file_name_old = 'occur_idx_' + str(self.idx_file_counter) + '.idx'
        
        if os.path.exists(file_name_old):
            file = open(file_name_old, 'rb')
        else:
            while True:
                file_name_new = 'occur_idx_' + str(self.idx_file_counter+1) + '.idx'
                if os.path.exists(file_name_new):
                    file = open(file_name_new, 'rb')
                    break
                self.idx_file_counter += 1
        
        file.read(position_in_file)  # lê tudo que há antes

        for i in range(num_of_occur):

            doc_id = int.from_bytes(file.read(4), byteorder='big')
            term_id = int.from_bytes(file.read(4), byteorder='big')
            term_freq = int.from_bytes(file.read(4), byteorder='big')

            tmp = TermOccurrence(doc_id, term_id, term_freq)
            occur_list.append(tmp)

        file.close()

        return occur_list

    def document_count_with_term(self, term: str) -> int:
        return self.dic_index[term].doc_count_with_term if term in self.dic_index else 0
