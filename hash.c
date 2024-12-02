#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define LOAD_FACTOR 0.75
#define ORDEM 4

typedef struct Node {
    char *key;
    double *value;
    struct Node *next;
} Node;

typedef struct HashTable {
    Node **buckets;
    size_t size;
    size_t capacity;
} HashTable;

typedef struct {
    double sequencial;
    char event_time[23];
    char event_type[17];
    char product_id[11];
    char category_id[20];
    char category_code[50];
    char brand[20];
    char price[10];
    char user_id[10];
    char user_session[37];
    int ativo;
} Registro;

typedef struct {
    double sequencial;
    char product_id[11];
    char brand[20];
    char price[10];
    int ativo;
} Produto;

typedef struct {
    double sequencial;
    char user_id[10];
    char event_type[17];
    char event_time[23];
    int ativo;
} Acesso;

typedef struct {
    double chave;
    long file_offset;
} Indice;

int hash(const char *key, size_t capacity);

void resize_table(HashTable *table);

void free_table(HashTable *table) {
    for (size_t i = 0; i < table->capacity; i++) {
        Node *node = table->buckets[i];
        while (node != NULL) {
            Node *temp = node;
            node = node->next;
            free(temp);
        }
    }
    free(table->buckets);
    free(table);
}

void remove_key(HashTable *table, const char *key) {
    int index = hash(key, table->capacity);
    Node *node = table->buckets[index];
    Node *prev = NULL;

    while (node != NULL && strcmp(node->key, key) != 0) {
        prev = node;
        node = node->next;
    }

    if (node == NULL) {
        return; // Chave não encontrada
    }

    if (prev == NULL) {
        table->buckets[index] = node->next;
    } else {
        prev->next = node->next;
    }
    free(node);
    table->size--;
}

double *search(HashTable *table, const char *key) {
    int index = hash(key, table->capacity);
    Node *node = table->buckets[index];

    while (node != NULL) {
        if (strcmp(node->key, key) == 0) {
            return node->value; // Chave encontrada
        }
        node = node->next;
    }
    return NULL; // Chave não encontrada
}


void insert(HashTable *table, const char *key, long offset) {
    if ((float)table->size / table->capacity > LOAD_FACTOR) {
        resize_table(table);
    }

    int index = hash(key, table->capacity);
    Node *new_node = malloc(sizeof(Node));
    new_node->key = strdup(key);

    // Armazenar o offset diretamente
    new_node->value = malloc(sizeof(long));
    *(long *)(new_node->value) = offset;

    new_node->next = table->buckets[index];
    table->buckets[index] = new_node;

    table->size++;
}

int hash(const char *key, size_t capacity) {
    long int hash = 0;
    int i = 0;

    while (key[i] != '\0') {
        hash = hash * 31 + key[i];
        i++;
    }
    return hash % capacity;
}

void resize_table(HashTable *table) {
    size_t new_capacity = table->capacity * 2;
    Node **new_buckets = calloc(new_capacity, sizeof(Node *));

    // Recalcular os índices e mover todos os elementos para a nova tabela
    for (size_t i = 0; i < table->capacity; i++) {
        Node *node = table->buckets[i];
        while (node != NULL) {
            int index = hash(node->key, new_capacity);
            Node *new_node = malloc(sizeof(Node));
            new_node->key = node->key;
            new_node->value = node->value;
            new_node->next = new_buckets[index];
            new_buckets[index] = new_node;

            Node *temp = node;
            node = node->next;
            free(temp);
        }
    }

    // Substituir a tabela antiga pela nova
    free(table->buckets);
    table->buckets = new_buckets;
    table->capacity = new_capacity;
}

char *custom_strsep(char **stringp, const char *delim) {
    char *start = *stringp;
    char *p;

    if (start == NULL) {
        return NULL;
    }

    p = strpbrk(start, delim);
    if (p) {
        *p = '\0';
        *stringp = p + 1;
    } else {
        *stringp = NULL;
    }

    return start;
}

void processar_linha(char *linha, Registro *registro) {
    char *token;
    int campo = 0;

    // Inicializar o registro com espaços e o campo `ativo` como zero
    memset(registro, ' ', sizeof(Registro));
    registro->ativo = 0;

    // Processar os campos da linha
    while ((token = custom_strsep(&linha, ",")) != NULL) {
        switch (campo) {
            case 0:
                strncpy(registro->event_time, token, sizeof(registro->event_time) - 1);
                registro->event_time[sizeof(registro->event_time) - 1] = '\0';
                break;
            case 1:
                strncpy(registro->event_type, token, sizeof(registro->event_type) - 1);
                registro->event_type[sizeof(registro->event_type) - 1] = '\0';
                break;
            case 2:
                strncpy(registro->product_id, token, sizeof(registro->product_id) - 1);
                registro->product_id[sizeof(registro->product_id) - 1] = '\0';
                break;
            case 3:
                strncpy(registro->category_id, token, sizeof(registro->category_id) - 1);
                registro->category_id[sizeof(registro->category_id) - 1] = '\0';
                break;
            case 4:
                strncpy(registro->category_code, token, sizeof(registro->category_code) - 1);
                registro->category_code[sizeof(registro->category_code) - 1] = '\0';
                break;
            case 5:
                strncpy(registro->brand, token, sizeof(registro->brand) - 1);
                registro->brand[sizeof(registro->brand) - 1] = '\0';
                break;
            case 6:
                strncpy(registro->price, token, sizeof(registro->price) - 1);
                registro->price[sizeof(registro->price) - 1] = '\0';
                break;
            case 7:
                strncpy(registro->user_id, token, sizeof(registro->user_id) - 1);
                registro->user_id[sizeof(registro->user_id) - 1] = '\0';
                break;
            case 8:
                strncpy(registro->user_session, token, sizeof(registro->user_session) - 1);
                registro->user_session[sizeof(registro->user_session) - 1] = '\0';
                break;
        }
        campo++;
    }
}

int ler_e_inserir_dados(const char *arquivo_csv, FILE *arquivo_produtos, FILE *arquivo_acessos, FILE *arquivo_indice_produtos, FILE *arquivo_indice_acessos, HashTable *table_acessos, HashTable *table_produtos) {
    FILE *arquivo_csv_f = fopen(arquivo_csv, "r");
    if (arquivo_csv_f == NULL) {
        printf("Erro ao abrir o arquivo CSV.\n");
        return 0;
    }

    char linha[1024];
    Registro registro_lido;
    Produto produto_lido;
    Acesso acesso_lido;
    Indice indice_produto, indice_acesso;
    double sequencial = 1;

    if (fgets(linha, sizeof(linha), arquivo_csv_f) == NULL) {
        printf("Erro ao ler o cabeçalho ou o arquivo está vazio.\n");
        fclose(arquivo_csv_f);
        return 0;
    }

    int contador = 0;

    while (fgets(linha, sizeof(linha), arquivo_csv_f) != NULL && contador < 1000000) {
        linha[strcspn(linha, "\n")] = '\0';
        processar_linha(linha, &registro_lido);

        registro_lido.sequencial = sequencial;
        sequencial++;

        produto_lido.sequencial = registro_lido.sequencial;
        strncpy(produto_lido.product_id, registro_lido.product_id, sizeof(produto_lido.product_id) - 1);
        strncpy(produto_lido.brand, registro_lido.brand, sizeof(produto_lido.brand) - 1);
        strncpy(produto_lido.price, registro_lido.price, sizeof(produto_lido.price) - 1);
        produto_lido.ativo = 1;

        long offset_produto = ftell(arquivo_produtos);
        fwrite(&produto_lido, sizeof(Produto), 1, arquivo_produtos);

        indice_produto.chave = produto_lido.sequencial;
        indice_produto.file_offset = offset_produto;
        fwrite(&indice_produto, sizeof(Indice), 1, arquivo_indice_produtos);

        acesso_lido.sequencial = registro_lido.sequencial;
        strncpy(acesso_lido.event_time, registro_lido.event_time, sizeof(acesso_lido.event_time) - 1);
        strncpy(acesso_lido.event_type, registro_lido.event_type, sizeof(acesso_lido.event_type) - 1);
        strncpy(acesso_lido.user_id, registro_lido.user_id, sizeof(acesso_lido.user_id) - 1);
        acesso_lido.ativo = 1;

        long offset_acesso = ftell(arquivo_acessos);
        fwrite(&acesso_lido, sizeof(Acesso), 1, arquivo_acessos);

        indice_acesso.chave = acesso_lido.sequencial;
        indice_acesso.file_offset = offset_acesso;
        fwrite(&indice_acesso, sizeof(Indice), 1, arquivo_indice_acessos);

        //insert(table_produtos, produto_lido.product_id, offset_produto);
       // insert(table_acessos, acesso_lido.user_id, offset_acesso);
        contador++;
    }

    fclose(arquivo_csv_f);
    return 1;
}

// Função para inserir registros na tabela hash
void inserir_registros_tabela_hash(HashTable *table_produtos, HashTable *table_acessos, FILE *arquivo_produtos, FILE *arquivo_acessos) {
    Produto produto;
    Acesso acesso;
    long offset_produto, offset_acesso;

    // Posiciona o cursor no início dos arquivos binários
    fseek(arquivo_produtos, 0, SEEK_SET);
    fseek(arquivo_acessos, 0, SEEK_SET);

    // Insere os registros de produtos na tabela hash
    while (fread(&produto, sizeof(Produto), 1, arquivo_produtos) == 1) {
        if (produto.ativo) {
            offset_produto = ftell(arquivo_produtos) - sizeof(Produto);
            insert(table_produtos, produto.product_id, offset_produto);
        }
    }

    // Insere os registros de acessos na tabela hash
    while (fread(&acesso, sizeof(Acesso), 1, arquivo_acessos) == 1) {
        if (acesso.ativo) {
            offset_acesso = ftell(arquivo_acessos) - sizeof(Acesso);
            insert(table_acessos, acesso.user_id, offset_acesso);
        }
    }
}

void delete_key(HashTable *table, const char *key) {
    // Obter índice do bucket para a chave
    int index = hash(key, table->capacity);

    Node *prev = NULL;
    Node *curr = table->buckets[index];

    // Percorrer a lista ligada no bucket
    while (curr != NULL) {
        // Verificar se a chave corresponde
        if (strcmp(curr->key, key) == 0) {
            // Caso seja o primeiro nó do bucket
            if (prev == NULL) {
                table->buckets[index] = curr->next;
            } else {
                prev->next = curr->next;
            }

            // Liberar memória do nó removido
            free(curr->key);
            free(curr->value);
            free(curr);
            table->size--; // Atualizar tamanho da tabela
            return;        // Finalizar a função
        }
        prev = curr;
        curr = curr->next;
    }

    // Se chegar aqui, a chave não foi encontrada
    printf("Chave '%s' não encontrada na tabela hash.\n", key);
}

long obter_tamanho_arquivo(const char *nome_arquivo) {
    FILE *arquivo = fopen(nome_arquivo, "rb");
    if (arquivo == NULL) {
        printf("Erro ao abrir o arquivo '%s'.\n", nome_arquivo);
        return -1;
    }
    fseek(arquivo, 0, SEEK_END);
    long tamanho = ftell(arquivo);
    fclose(arquivo);
    return tamanho;
}

int buscar_com_hash(HashTable *table, const char *key, const char *arquivo_dados, void *registro, size_t tamanho_registro) {
    long *offset = (long *)search(table, key);
    if (offset == NULL) {
        printf("Chave '%s' não encontrada na tabela hash.\n", key);
        return 0; // Chave não encontrada
    }

    long tamanho_arquivo = obter_tamanho_arquivo(arquivo_dados);
    if (*offset >= tamanho_arquivo) {
        printf("Offset obtido (%ld) está fora dos limites do arquivo (tamanho: %ld).\n", *offset, tamanho_arquivo);
        return 0; // Offset inválido
    }

	if (*offset == 0) {
        printf("Offset obtido (%ld).\n", *offset);
        return 0; // Offset inválido
    }

    FILE *dados = fopen(arquivo_dados, "rb");
    if (!dados) {
        printf("Erro ao abrir o arquivo de dados '%s'.\n", arquivo_dados);
        return 0; // Erro ao abrir o arquivo
    }

    if (fseek(dados, *offset, SEEK_SET) != 0) {
        printf("Erro ao posicionar cursor no arquivo (offset: %ld).\n", *offset);
        fclose(dados);
        return 0;
    }

    memset(registro, 0, tamanho_registro); // Inicializar a estrutura para evitar lixo na memória
    size_t read_size = fread(registro, tamanho_registro, 1, dados);
    if (read_size != 1) {
        printf("Erro ao ler o registro do arquivo (tamanho do registro: %zu bytes, lido: %zu).\n", tamanho_registro, read_size * tamanho_registro);
        fclose(dados);
        return 0;
    }

    fclose(dados);
    return 1; // Sucesso
}

void inserir_novo_produto(HashTable *table_produtos, FILE *arquivo_dados, Produto novo_produto) {
    Produto produto;
    long offset;

    printf("Iniciando inserção do produto: %s\n", novo_produto.product_id);

    // Verifica se há registros inativos no arquivo
    fseek(arquivo_dados, 0, SEEK_SET); // Posiciona no início do arquivo
    while (fread(&produto, sizeof(Produto), 1, arquivo_dados) == 1) {
        offset = ftell(arquivo_dados) - sizeof(Produto); // Calcula o offset atual

        if (produto.ativo == 0 && strcmp(produto.product_id, novo_produto.product_id) == 0) {
            // Registro inativo encontrado, reutilizar o espaço
            novo_produto.sequencial = produto.sequencial; // Mantém o sequencial original
            novo_produto.ativo = 1; // Marca como ativo

            printf("Registro inativo encontrado, atualizando: Offset: %ld\n", offset);

            if (fseek(arquivo_dados, offset, SEEK_SET) != 0) {
                printf("Erro ao reposicionar cursor no offset: %ld\n", offset);
                perror("fseek");
                return;
            }

            // Imprime os dados a serem gravados
            printf("Dados a serem gravados - ID: %s, Brand: %s, Price: %s, Ativo: %d\n",
                   novo_produto.product_id, novo_produto.brand, novo_produto.price, novo_produto.ativo);

            if (fwrite(&novo_produto, sizeof(Produto), 1, arquivo_dados) != 1) {
                printf("Erro ao escrever o produto no offset: %ld\n", offset);
                perror("fwrite");
                return;
            }

            fflush(arquivo_dados); // Garante que os dados sejam gravados
            printf("Produto sobrescrito no arquivo: ID: %s, Offset: %ld\n", novo_produto.product_id, offset);
            insert(table_produtos, novo_produto.product_id, offset); // Atualiza o índice
            printf("Produto atualizado no arquivo, ID: %s\n", novo_produto.product_id);
            return;
        }
    }

    // Caso nenhum registro inativo seja encontrado, insere o novo produto no final do arquivo
    fseek(arquivo_dados, 0, SEEK_END); // Vai até o final do arquivo
    offset = ftell(arquivo_dados); // Obtém o offset do final do arquivo
    novo_produto.sequencial = offset / sizeof(Produto) + 1; // Calcula o sequencial baseado no offset
    novo_produto.ativo = 1; // Marca como ativo

    printf("Inserindo novo produto no final do arquivo: Offset: %ld\n", offset);

    if (fwrite(&novo_produto, sizeof(Produto), 1, arquivo_dados) != 1) {
        printf("Erro ao escrever novo produto no offset: %ld\n", offset);
        perror("fwrite");
        return;
    }

    fflush(arquivo_dados); // Garante que os dados sejam gravados no disco
    insert(table_produtos, novo_produto.product_id, offset); // Adiciona ao índice
    printf("Novo produto inserido no arquivo, ID: %s\n", novo_produto.product_id);
}

int remover_registro(HashTable *table, const char *key, const char *arquivo_dados, size_t tamanho_registro) {
    long *offset = (long *)search(table, key);
    if (offset == NULL) {
        printf("Chave '%s' não encontrada na tabela hash.\n", key);
        return 0; // Chave não encontrada
    }

    FILE *dados = fopen(arquivo_dados, "rb+"); // Abre o arquivo para leitura e escrita
    if (!dados) {
        printf("Erro ao abrir o arquivo de dados '%s'.\n", arquivo_dados);
        return 0;
    }

    if (fseek(dados, *offset, SEEK_SET) != 0) {
        printf("Erro ao posicionar cursor no arquivo (offset: %ld).\n", *offset);
        fclose(dados);
        return 0;
    }

    char buffer[tamanho_registro];
    if (fread(buffer, tamanho_registro, 1, dados) != 1) {
        printf("Erro ao ler o registro no offset %ld.\n", *offset);
        fclose(dados);
        return 0;
    }

    // Marca o registro como inativo
    int *ativo_ptr = (int *)(buffer + tamanho_registro - sizeof(int));
    *ativo_ptr = 0;

    if (fseek(dados, *offset, SEEK_SET) != 0) {
        printf("Erro ao reposicionar cursor no arquivo (offset: %ld).\n", *offset);
        fclose(dados);
        return 0;
    }

    if (fwrite(buffer, tamanho_registro, 1, dados) != 1) {
        printf("Erro ao escrever o registro atualizado no offset %ld.\n", *offset);
        fclose(dados);
        return 0;
    }

    fclose(dados);

    // Remove a chave da tabela hash
    remove_key(table, key);

    printf("Registro com a chave '%s' removido com sucesso.\n", key);
    return 1; // Sucesso
}

void print_hash_table_records(HashTable *table, const char *key, FILE *dados) {
    if (table == NULL || key == NULL) {
        printf("Erro: Tabela ou chave inválida.\n");
        return;
    }

    size_t index = hash(key, table->capacity);
    if (index >= table->capacity) {
        printf("Erro: Índice fora dos limites da tabela hash.\n");
        return;
    }

    Node *current = table->buckets[index];
    if (current == NULL) {
        printf("Nenhum registro encontrado para a chave '%s'.\n", key);
        return;
    }

    int found = 0;
    int count = 0;
    Produto produto;

    printf("Registros associados a chave '%s':\n", key);
    while (current != NULL) {
        if (strcmp(current->key, key) == 0) {
            found = 1;
            count++;
            printf("  [%d] Valor associado: %ld\n", count, *(long *)current->value);

            // Posiciona o cursor no arquivo
            fseek(dados, *(long *)current->value, SEEK_SET);

            // Inicializa a estrutura
            memset(&produto, 0, sizeof(Produto));

            // Lê o registro
            if (fread(&produto, sizeof(Produto), 1, dados) == 1) {
            	printf("    Sequencial: %.0f\n", produto.sequencial);
                printf("    Produto ID: %s\n", produto.product_id);
                printf("    Brand: %s\n", produto.brand);
                printf("    Preco: %s\n", produto.price);
            } else {
                printf("    Erro ao ler o registro associado.\n");
            }
        }
        current = current->next;
    }

    if (!found) {
        printf("Nenhum registro encontrado para a chave '%s'.\n", key);
    } else {
        printf("Total de registros encontrados: %d\n", count);
    }
}



int main() {
    // Inicializando tabelas hash
    HashTable *table_produtos = malloc(sizeof(HashTable));
    table_produtos->capacity = 10;
    table_produtos->size = 0;
    table_produtos->buckets = calloc(table_produtos->capacity, sizeof(Node *));

    HashTable *table_acessos = malloc(sizeof(HashTable));
    table_acessos->capacity = 10;
    table_acessos->size = 0;
    table_acessos->buckets = calloc(table_acessos->capacity, sizeof(Node *));

    // Abrindo arquivos
    FILE *arquivo_produtos = fopen("produtos.dat", "wb+");
    FILE *arquivo_indice_produtos = fopen("indice_produtos.dat", "wb+");
    FILE *arquivo_acessos = fopen("acessos.dat", "wb+");
    FILE *arquivo_indice_acessos = fopen("indice_acessos.dat", "wb+");

    if (arquivo_produtos == NULL || arquivo_acessos == NULL || arquivo_indice_produtos == NULL || arquivo_indice_acessos == NULL) {
        printf("Erro ao abrir arquivos.\n");
        return 1;
    }

    // Lendo e inserindo dados
    if (!ler_e_inserir_dados("products.csv", arquivo_produtos, arquivo_acessos, arquivo_indice_produtos, arquivo_indice_acessos, table_acessos, table_produtos)) {
        return 1;
    }
    clock_t inicio, fim;
    double tempo_execucao;
	inicio = clock();
	inserir_registros_tabela_hash(table_produtos, table_acessos, arquivo_produtos, arquivo_acessos);
	fim = clock();

	tempo_execucao = (double)(fim - inicio) / CLOCKS_PER_SEC;

    printf("Tempo para criar a tabela hash: %.6f segundos\n", tempo_execucao);
    // Fechando arquivos após a escrita
    fclose(arquivo_produtos);
    fclose(arquivo_indice_produtos);
    fclose(arquivo_acessos);
    fclose(arquivo_indice_acessos);

    // Reabrindo arquivos para leitura
    arquivo_produtos = fopen("produtos.dat", "rb+");
    arquivo_acessos = fopen("acessos.dat", "rb+");
    if (arquivo_produtos == NULL || arquivo_acessos == NULL) {
        printf("Erro ao reabrir arquivos.\n");
        return 1;
    }
    // Buscando um produto
    /*Produto produto_encontrado;
    if (buscar_com_hash(table_produtos, "44600062", "produtos.dat", &produto_encontrado, sizeof(Produto))) {
        printf("Produto encontrado:\n");
        printf("Sequencial: %.0f\n", produto_encontrado.sequencial);
        printf("Product ID: %s\n", produto_encontrado.product_id);
        printf("Brand: %s\n", produto_encontrado.brand);
        printf("Price: %s\n", produto_encontrado.price);
        printf("Ativo: %d\n", produto_encontrado.ativo);
    } else {
        printf("Produto não encontrado.\n");
    */

    // Buscando um acesso
   /*Acesso acesso_encontrado;
    if (buscar_com_hash(table_acessos, "535871217", "acessos.dat", &acesso_encontrado, sizeof(Acesso))) {
        printf("Acesso encontrado:\n");
        printf("Sequencial: %.0f\n", acesso_encontrado.sequencial);
        printf("User ID: %s\n", acesso_encontrado.user_id);
        printf("Event Time: %s\n", acesso_encontrado.event_time);
        printf("Event Type: %s\n", acesso_encontrado.event_type);
        printf("Ativo: %d\n", acesso_encontrado.ativo);
    } else {
        printf("Acesso não encontrado.\n");
    }


    //remover_registro(table_produtos, "44600062", "produtos.dat", sizeof(Produto));
    //remover_registro(table_acessos, "535871217", "acessos.dat", sizeof(Acesso));


	Produto produto_encontrado1;
    if (buscar_com_hash(table_produtos, "44600062", "produtos.dat", &produto_encontrado1, sizeof(Produto))) {
        printf("Produto encontrado:\n");
        printf("Sequencial: %.0f\n", produto_encontrado1.sequencial);
        printf("Product ID: %s\n", produto_encontrado1.product_id);
        printf("Brand: %s\n", produto_encontrado1.brand);
        printf("Price: %s\n", produto_encontrado1.price);
        printf("Ativo: %d\n", produto_encontrado1.ativo);
    } else {
        printf("Produto não encontrado.\n");
    }

    Produto novo_produto;
    novo_produto.ativo=0;
    strcpy(novo_produto.brand, "Huawei");
    strcpy(novo_produto.product_id, "44600062");
    strcpy(novo_produto.price, "57.90");

    inserir_novo_produto(table_produtos, arquivo_produtos, novo_produto);

    Produto produto_encontrado2;
    if (buscar_com_hash(table_produtos, "44600062", "produtos.dat", &produto_encontrado2, sizeof(Produto))) {
        printf("Produto encontrado:\n");
        printf("Sequencial: %.0f\n", produto_encontrado2.sequencial);
        printf("Product ID: %s\n", produto_encontrado2.product_id);
        printf("Brand: %s\n", produto_encontrado2.brand);
        printf("Price: %s\n", produto_encontrado2.price);
        printf("Ativo: %d\n", produto_encontrado2.ativo);
    } else {
        printf("Produto não encontrado.\n");
    }
    */

   // print_hash_table_records(table_produtos, "1003306", arquivo_produtos);

   	fclose(arquivo_produtos);
    fclose(arquivo_acessos);

    free_table(table_produtos);
    free_table(table_acessos);
    free(table_acessos);
    free(table_produtos);

    return 0;
}

