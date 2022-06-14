# Um Job deve saber:

# - Atualizar o seu tempo interno corretamente
# dependendo do estado em que estiver
# - Saber calcular os seus tempos de fila e execução
# - Saber se está em execução ou não


# Casos de uso com Job (a julgar o que é
# responsabilidade do modelo):

# - Criado novo job para executar um caso
# - Inicializado com atributos default
# - Submetido à fila
# - Atualização de estado
# - Possivel timeout
# - Deleção
# - Saída da fila por sucesso ou por deleção
# - Erro em qualquer etapa
