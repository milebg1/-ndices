#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ORDEM 4

typedef struct {
    double sequencial;
    char event_time[50];
    char event_type[50];
    char product_id[50];
    char category_id[50];
    char category_code[50];
    char brand[50];
    char price[50];
    char user_id[50];
    char user_session[50];
    int ativo;
} Registro;

typedef struct {
    double sequencial;
    char product_id[50];
    char brand[50];
    char price[50];
    int ativo;
} Produto;

typedef struct {
    double sequencial;
    char user_id[50];
    char event_type[50];
    char event_time[50];
    int ativo;
} Acesso;

typedef struct {
    double chave;
    long file_offset;
} Indice;

typedef struct BTreeNode {
    int num_chaves;
    Indice chaves[ORDEM - 1];
    struct BTreeNode *filhos[ORDEM];
    int folha;
} BTreeNode;

BTreeNode *criarNo(int folha) {
    BTreeNode *no = (BTreeNode *)malloc(sizeof(BTreeNode));
    no->num_chaves = 0;
    no->folha = folha;
    for (int i = 0; i < ORDEM; i++) {
        no->filhos[i] = NULL;
    }
    return no;
}

BTreeNode *buscar(BTreeNode *raiz, double chave){
    int i = 0;
    while (i < raiz->num_chaves && chave > raiz->chaves[i].chave){
        i++;
    }

    if(i<raiz->num_chaves && chave == raiz->chaves[i].chave){
        return raiz;
    }

    if(raiz->folha){
        return NULL;
    }

    return buscar(raiz->filhos[i], chave);
}

void dividirFilho(BTreeNode *pai, int i);

void inserirNaoCheio(BTreeNode *no, Indice novo_indice){
    int i = no->num_chaves - 1;

    if(no->folha){
        while(i >= 0 && novo_indice.chave < no->chaves[i].chave){
            no->chaves[i + 1] = no->chaves[i];
            i--;
        }
        no->chaves[i + 1] = novo_indice;
        no->num_chaves++;
    }else{
        while(i >= 0 && novo_indice.chave < no->chaves[i].chave){
            i--;
        }
        i++;
        if(no->filhos[i]->num_chaves == ORDEM - 1){
            dividirFilho(no, i);
            if(novo_indice.chave > no->chaves[i].chave){
                i++;
            }
        }
        inserirNaoCheio(no->filhos[i], novo_indice);
    }
}

void dividirFilho(BTreeNode *pai, int i){
    BTreeNode *filho_cheio = pai->filhos[i];
    BTreeNode *novo_no = criarNo(filho_cheio->folha);

    novo_no->num_chaves = (ORDEM - 1)/2;

    for (int j = 0; j < novo_no->num_chaves; j++){
        novo_no->chaves[j] = filho_cheio->chaves[j + (ORDEM/2)];
    }

    if(!filho_cheio->folha){
        for(int j = 0; j <= novo_no->num_chaves; j++){
            novo_no->filhos[j] = filho_cheio->filhos[j + (ORDEM/2)];
        }
    }

    filho_cheio->num_chaves = (ORDEM - 1)/2;

    for(int j = pai->num_chaves; j >= i + 1; j--){
        pai->filhos[j + 1] = pai->filhos[j];
    }
    pai->filhos[i + 1] = novo_no;

    for(int j = pai->num_chaves - 1; j >= i; j--){
        pai->chaves[j + 1] = pai->chaves[j];
    }
    pai->chaves[i] = filho_cheio->chaves[(ORDEM - 1)/2];
    pai->num_chaves++;
}

void inserir(BTreeNode **raiz, Indice novo_indice){
    BTreeNode *r =*raiz;
    if(r->num_chaves == ORDEM - 1){
        BTreeNode *nova_raiz = criarNo(0);
        nova_raiz->filhos[0] = r;
        dividirFilho(nova_raiz, 0);
        inserirNaoCheio(nova_raiz, novo_indice);
        *raiz = nova_raiz;
    }else{
        inserirNaoCheio(r, novo_indice);
    }
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

    memset(registro, 0, sizeof(Registro));

    while ((token = custom_strsep(&linha, ",")) != NULL) {
        switch (campo) {
            case 0: strncpy(registro->event_time, token, sizeof(registro->event_time) - 1); break;
            case 1: strncpy(registro->event_type, token, sizeof(registro->event_type) - 1); break;
            case 2: strncpy(registro->product_id, token, sizeof(registro->product_id) - 1); break;
            case 3: strncpy(registro->category_id, token, sizeof(registro->category_id) - 1); break;
            case 4: strncpy(registro->category_code, token, sizeof(registro->category_code) - 1); break;
            case 5: strncpy(registro->brand, token, sizeof(registro->brand) - 1); break;
            case 6: strncpy(registro->price, token, sizeof(registro->price) - 1); break;
            case 7: strncpy(registro->user_id, token, sizeof(registro->user_id) - 1); break;
            case 8: strncpy(registro->user_session, token, sizeof(registro->user_session) - 1); break;
        }
        campo++;
    }
}

void exibir_registro_acesso(const Acesso *acesso) {
    //printf("Sequencial: %.0f | Event Time: %s | Event Type: %s | User ID: %s \n",
    //        acesso->sequencial,
    //       strlen(acesso->event_time) > 0 ? acesso->event_time : "N/A",
    //		strlen(acesso->event_type) > 0 ? acesso->event_type : "N/A",
    //        strlen(acesso->user_id) > 0 ? acesso->user_id : "N/A");
}

int ler_e_inserir_dados(const char *arquivo_csv, FILE *arquivo_produtos, FILE *arquivo_acessos, FILE *arquivo_indice_produtos, FILE *arquivo_indice_acessos) {
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

    while (fgets(linha, sizeof(linha), arquivo_csv_f) != NULL && contador<100000) {
        linha[strcspn(linha, "\n")] = '\0';
        processar_linha(linha, &registro_lido);
        // printf("Registro encontrado: %.0f | %s | %s | %s \n", registro_lido.sequencial, registro_lido.event_time, registro_lido.user_id, registro_lido.event_type);

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

        exibir_registro_acesso(&acesso_lido);

        contador++;
    }

    fclose(arquivo_csv_f);
    return 1;
}
BTreeNode *buscar_produto(BTreeNode *raiz, double chave) {
    return buscar(raiz, chave);
}

void consultar_produto(BTreeNode *raiz, double chave) {
    BTreeNode *resultado = buscar_produto(raiz, chave);
    if (resultado == NULL) {
        printf("Produto não encontrado!\n");
    } else {
        printf("Produto encontrado com sequencial: %.0f\n", chave);
    }
}

BTreeNode *carregarIndiceProdutos(const char *arquivo_indice){
    FILE *arquivo = fopen(arquivo_indice, "rb");
    if(arquivo == NULL){
        printf("Erro ao abrir o arquivo de índice de produtos.\n");
        return NULL;
    }

    BTreeNode *raiz = criarNo(1);

    Indice indice_lido;
    while(fread(&indice_lido, sizeof(Indice), 1, arquivo)== 1){
        Indice novo_indice = indice_lido;
        inserir(&raiz, novo_indice);
    }

    fclose(arquivo);
    return raiz;
}
void consultar_produto_com_indice(BTreeNode *raiz, double chave){
    BTreeNode *resultado = buscar_produto(raiz, chave);
    if (resultado == NULL) {
            printf("\n");
        printf("Produto não encontrado!\n");
    }else
        printf("\n");
        printf("Produto encontrado com sequencial: %.0f\n", chave);

        FILE *arquivo_produtos = fopen("produtos.dat", "rb");
        if (arquivo_produtos == NULL) {
            printf("Erro ao abrir o arquivo de produtos.\n");
            return;
        }

        Produto produto_lido;
        fseek(arquivo_produtos, resultado->chaves[0].file_offset, SEEK_SET);
        fread(&produto_lido, sizeof(Produto), 1, arquivo_produtos);
        fclose(arquivo_produtos);

        printf("Detalhes do Produto:\n");
        printf("Sequencial: %.0f\n", produto_lido.sequencial);
        printf("Product ID: %s\n", produto_lido.product_id);
        printf("Brand: %s\n", produto_lido.brand);
        printf("Price: %s\n", produto_lido.price);

}

void liberarArvore(BTreeNode *no){
    if(no == NULL){
        return;
    }
    if (!no->folha) {
        for (int i = 0; i <= no->num_chaves; i++){
            liberarArvore(no->filhos[i]);
        }
    }
    free(no);
}

void encontrar_maior_valor(BTreeNode *no, double chave_min, double chave_max, Produto *maior_produto){
    if(no == NULL){
        return;
    }

    int i = 0;
    while(i < no->num_chaves && no->chaves[i].chave < chave_min){
        i++;
    }

    while(i < no->num_chaves && no->chaves[i].chave <= chave_max){
        FILE *arquivo_produtos = fopen("produtos.dat", "rb");
        if (arquivo_produtos == NULL) {
            printf("Erro ao abrir o arquivo de produtos.\n");
            return;
        }

        Produto produto_lido;
        fseek(arquivo_produtos, no->chaves[i].file_offset, SEEK_SET);
        fread(&produto_lido, sizeof(Produto), 1, arquivo_produtos);
        fclose(arquivo_produtos);

        double preco_atual = atof(produto_lido.price);
        double preco_maior = atof(maior_produto->price);
        if(preco_atual > preco_maior){
            *maior_produto = produto_lido;
        }

        i++;
    }

    if(!no->folha){
        for(int j = 0; j <= no->num_chaves; j++){
            if ((j == 0 && chave_min <= no->chaves[0].chave) ||
                (j> 0 && no->chaves[j - 1].chave <chave_max)){
                encontrar_maior_valor(no->filhos[j], chave_min, chave_max, maior_produto);
            }
        }
    }
}

void encontrarProdutoMaisBaratoEntreChaves(BTreeNode *no, double chave_min, double chave_max, Produto *produtoMaisBarato){
    if(no == NULL){
        return;
    }

    for(int i = 0; i < no->num_chaves; i++){
        if(no->chaves[i].chave >= chave_min && no->chaves[i].chave <=chave_max){
            FILE *arquivo_produtos = fopen("produtos.dat", "rb");
            if (arquivo_produtos == NULL) {
                printf("Erro ao abrir o arquivo de produtos.\n");
                return;
            }

            Produto produtoAtual;
            fseek(arquivo_produtos, no->chaves[i].file_offset, SEEK_SET);
            fread(&produtoAtual, sizeof(Produto), 1, arquivo_produtos);
            fclose(arquivo_produtos);

            double precoAtual = atof(produtoAtual.price);
            double precoMaisBarato = atof(produtoMaisBarato->price);

            if (produtoAtual.ativo && (produtoMaisBarato->ativo == 0 || precoAtual < precoMaisBarato)) {
                *produtoMaisBarato = produtoAtual;
            }
        }

        if(no->chaves[i].chave >chave_max){
            break;
        }
    }

    for(int i = 0; i <= no->num_chaves; i++){
        if(no->filhos[i] != NULL){
            encontrarProdutoMaisBaratoEntreChaves(no->filhos[i], chave_min, chave_max, produtoMaisBarato);
        }
    }
}

void contarMarcaEmIntervalo(BTreeNode *no, double chave_min, double chave_max, int *contador, char *marca_mais_frequente) {
    if(no == NULL){
        return;
    }

    for(int i = 0; i < no->num_chaves; i++){
        if(no->chaves[i].chave >= chave_min && no->chaves[i].chave <= chave_max){
            FILE *arquivo_produtos = fopen("produtos.dat", "rb");
            if(arquivo_produtos == NULL){
                printf("Erro ao abrir o arquivo de produtos.\n");
                return;
            }

            Produto produto_lido;
            fseek(arquivo_produtos, no->chaves[i].file_offset, SEEK_SET);
            fread(&produto_lido, sizeof(Produto), 1, arquivo_produtos);
            fclose(arquivo_produtos);

            if(produto_lido.brand[0] != '\0'){
                if(strcmp(produto_lido.brand, marca_mais_frequente) == 0){
                    (*contador)++;
                }else if (*contador == 0){
                    strcpy(marca_mais_frequente, produto_lido.brand);
                    *contador = 1;
                }else{
                    if(*contador > 0){
                        (*contador)++;
                    }else{
                        strcpy(marca_mais_frequente, produto_lido.brand);
                        *contador = 1;
                    }
                }
            }
        }
    }

    if(!no->folha){
        for(int i = 0; i <= no->num_chaves; i++){
            contarMarcaEmIntervalo(no->filhos[i], chave_min, chave_max, contador, marca_mais_frequente);
        }
    }
}

void remover(BTreeNode *no, double chave) {
    int i = 0;
    if (no->folha) {
        while (i < no->num_chaves && no->chaves[i].chave != chave){
            i++;
        }
        if (i < no->num_chaves) {
            for (int j = i; j < no->num_chaves - 1; j++){
                no->chaves[j] = no->chaves[j + 1];
            }
            no->num_chaves--;
        }
        return;
    }
    while (i < no->num_chaves && chave > no->chaves[i].chave) {
        i++;
    }
    if (i < no->num_chaves && chave == no->chaves[i].chave) {
        if (no->filhos[i]->num_chaves >= (ORDEM + 1) / 2) {
            BTreeNode *filho_esquerdo = no->filhos[i];
            BTreeNode *sucessor = filho_esquerdo;
            while (!sucessor->folha) {
                sucessor = sucessor->filhos[sucessor->num_chaves];
            }
            no->chaves[i] = sucessor->chaves[sucessor->num_chaves - 1];
            remover(filho_esquerdo, sucessor->chaves[sucessor->num_chaves - 1].chave);
        }
        else if (i + 1 < no->num_chaves && no->filhos[i + 1]->num_chaves >= (ORDEM + 1) / 2) {
            BTreeNode *filho_direito = no->filhos[i + 1];
            BTreeNode *predecessor = filho_direito;
            while (!predecessor->folha) {
                predecessor = predecessor->filhos[0];
            }
            no->chaves[i] = predecessor->chaves[0];
            remover(filho_direito, predecessor->chaves[0].chave);
        }
        else {
            BTreeNode *filho_esquerdo = no->filhos[i];
            BTreeNode *filho_direito = no->filhos[i + 1];
            filho_esquerdo->chaves[filho_esquerdo->num_chaves] = no->chaves[i];
            for (int j = 0; j < filho_direito->num_chaves; j++) {
                filho_esquerdo->chaves[filho_esquerdo->num_chaves + 1 + j] = filho_direito->chaves[j];
            }
            for (int j = 0; j <= filho_direito->num_chaves; j++) {
                filho_esquerdo->filhos[filho_esquerdo->num_chaves + 1 + j] = filho_direito->filhos[j];
            }
            filho_esquerdo->num_chaves += filho_direito->num_chaves + 1;
            free(filho_direito);
            for (int j = i + 1; j < no->num_chaves; j++) {
                no->chaves[j - 1] = no->chaves[j];
                no->filhos[j] = no->filhos[j + 1];
            }
            no->num_chaves--;
            remover(filho_esquerdo, chave);
        }
    } else {
        remover(no->filhos[i], chave);
    }
}

void fundirNos(BTreeNode *pai, int i) {
    BTreeNode *filho = pai->filhos[i];
    BTreeNode *irmao_adjacente = pai->filhos[i + 1];

    filho->chaves[ORDEM / 2 - 1] = pai->chaves[i];

    for (int j = 0; j < irmao_adjacente->num_chaves; j++) {
        filho->chaves[j + ORDEM / 2] = irmao_adjacente->chaves[j];
    }
    if(!filho->folha){
        for (int j = 0; j <= irmao_adjacente->num_chaves; j++){
            filho->filhos[j + ORDEM / 2] = irmao_adjacente->filhos[j];
        }
    }

    filho->num_chaves += irmao_adjacente->num_chaves + 1;

    for(int j = i + 1; j < pai->num_chaves; j++){
        pai->chaves[j - 1] = pai->chaves[j];
        pai->filhos[j] = pai->filhos[j + 1];
    }
    pai->num_chaves--;

    free(irmao_adjacente);
}

int main() {

    FILE *arquivo_produtos, *arquivo_acessos;
    FILE *arquivo_indice_produtos, *arquivo_indice_acessos;

    arquivo_produtos = fopen("produtos.dat", "wb+");
    arquivo_indice_produtos = fopen("indice_produtos.dat", "wb+");
    arquivo_acessos = fopen("acessos.dat", "wb+");
    arquivo_indice_acessos = fopen("indice_acessos.dat", "wb+");

    if (arquivo_produtos == NULL || arquivo_acessos == NULL || arquivo_indice_produtos == NULL || arquivo_indice_acessos == NULL) {
        printf("Erro ao abrir arquivos.\n");
        return 1;
    }

   if (!ler_e_inserir_dados("products.csv", arquivo_produtos, arquivo_acessos, arquivo_indice_produtos, arquivo_indice_acessos)) {
        return 1;
    }

    fclose(arquivo_produtos);
    fclose(arquivo_indice_produtos);
    fclose(arquivo_acessos);
    fclose(arquivo_indice_acessos);

    BTreeNode *arvore_produtos = carregarIndiceProdutos("indice_produtos.dat");

    double chave = 25;
    consultar_produto_com_indice(arvore_produtos, chave);

    double chave_a_remover = 20;
    remover(arvore_produtos, chave_a_remover);

    double chave_min = 25;
    double chave_max = 30;

    Produto maior_produto;
    memset(&maior_produto, 0, sizeof(Produto));
    strcpy(maior_produto.price, "0");

    encontrar_maior_valor(arvore_produtos, chave_min, chave_max, &maior_produto);

    if (atof(maior_produto.price) > 0) {
            printf("\n");
        printf("Produto com maior valor no intervalo [%.0f, %.0f]:\n", chave_min, chave_max);
        printf("Sequencial: %.0f\n", maior_produto.sequencial);
        printf("Product ID: %s\n", maior_produto.product_id);
        printf("Brand: %s\n", maior_produto.brand);
        printf("Price: %s\n", maior_produto.price);
    } else {
        printf("Nenhum produto encontrado no intervalo [%.0f, %.0f].\n", chave_min, chave_max);
    }

    Produto produtoMaisBarato;
    memset(&produtoMaisBarato, 0, sizeof(Produto));

    encontrarProdutoMaisBaratoEntreChaves(arvore_produtos, chave_min, chave_max, &produtoMaisBarato);

    if(produtoMaisBarato.ativo){
         printf("\nProduto com menor valor no intervalo [%.0f, %.0f]:\n", chave_min, chave_max);
         printf("Sequencial: %.0f\n", produtoMaisBarato.sequencial);
         printf("Product ID: %s\n", produtoMaisBarato.product_id);
         printf("Brand: %s\n", produtoMaisBarato.brand);
         printf("Price: %s\n", produtoMaisBarato.price);
    } else {
         printf("\nNenhum produto encontrado entre %.0f e %.0f.\n", chave_min, chave_max);
    }

    char marca_mais_frequente[50] = "";
    int contador = 0;

    contarMarcaEmIntervalo(arvore_produtos, chave_min, chave_max, &contador, marca_mais_frequente);

    if(contador>0){
        printf("\n");
        printf("A marca mais frequente entre as chaves %.0f e %.0f eh: %s com %d ocorrencias.\n",
               chave_min, chave_max, marca_mais_frequente, contador);
    } else {
        printf("Nenhuma marca encontrada no intervalo.\n");
    }

    liberarArvore(arvore_produtos);

    return 0;

}
