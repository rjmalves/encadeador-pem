from abc import abstractmethod
from typing import Dict, List, Callable, Union
from os.path import join
from os import chdir, listdir
from encadeador.controladores.avaliadorcaso import AvaliadorCaso
from encadeador.controladores.flexibilizadorcaso import Flexibilizador
from encadeador.controladores.monitorjob import MonitorJob

from encadeador.modelos.caso import Caso, CasoNEWAVE, CasoDECOMP
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.dadosjob import DadosJob
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.modelos.job import Job
from encadeador.modelos.regrareservatorio import RegraReservatorio
from encadeador.modelos.transicaocaso import TransicaoCaso
from encadeador.modelos.transicaojob import TransicaoJob
from encadeador.controladores.armazenadorcaso import ArmazenadorCaso
from encadeador.controladores.preparadorcaso import PreparadorCaso
from encadeador.controladores.sintetizadorcaso import SintetizadorCaso
from encadeador.controladores.sintetizadorcaso import SintetizadorNEWAVE
from encadeador.utils.log import Log
from encadeador.utils.event import Event


class MonitorCaso:
    """
    Responsável por monitorar a execução
    de um caso através dos seus jobs.
    Implementa o State Pattern para coordenar a execução do caso,
    adquirindo informações do estado dos jobs por meio do Observer Pattern.
    """

    def __init__(self, caso: Caso):
        self._caso = caso
        self._armazenador = ArmazenadorCaso(caso)
        self._avaliador = AvaliadorCaso.factory(caso)
        self._sintetizador = SintetizadorCaso.factory(caso)
        self._job_atual: Job = None  # type: ignore
        self._monitor_job_atual: MonitorJob = None  # type: ignore
        self._transicao_caso = Event()

    @staticmethod
    def factory(caso: Caso) -> "MonitorCaso":
        if isinstance(caso, CasoNEWAVE):
            return MonitorNEWAVE(caso)
        elif isinstance(caso, CasoDECOMP):
            return MonitorDECOMP(caso)
        else:
            raise ValueError(f"Caso do tipo {type(caso)} não suportado")

    def callback_evento(self, evento: Union[TransicaoJob, TransicaoCaso]):
        """
        Esta função é usada para implementar o Observer Pattern.
        Quando chamada, significa que ocorreu algo com o job do caso
        em um GerenciadorFila ou algo de interesse aconteceu com o caso em si
        e o monitor deve reagir atualizando os campos adequados nos objetos.

        :param evento: O evento ocorrido com o job ou caso
        :type evento: Union[TransicaoJob, TransicaoCaso]
        """
        self._regras()[evento]()

    @property
    @abstractmethod
    def caminho_job(self) -> str:
        pass

    @property
    @abstractmethod
    def nome_job(self) -> str:
        pass

    def _regras(
        self,
    ) -> Dict[Union[TransicaoJob, TransicaoCaso], Callable]:
        return {
            TransicaoCaso.CRIADO: self._handler_criado,
            (
                TransicaoCaso.PREPARA_EXECUCAO_SOLICITADA
            ): self._handler_prepara_execucao_solicitada,
            (
                TransicaoCaso.PREPARA_EXECUCAO_SUCESSO
            ): self._handler_prepara_execucao_sucesso,
            (
                TransicaoCaso.PREPARA_EXECUCAO_ERRO
            ): self._handler_prepara_execucao_erro,
            (
                TransicaoCaso.INICIO_EXECUCAO_SOLICITADA
            ): self._handler_inicio_execucao_solicitada,
            (
                TransicaoCaso.INICIO_EXECUCAO_SUCESSO
            ): self._handler_inicio_execucao_sucesso,
            (
                TransicaoCaso.INICIO_EXECUCAO_ERRO
            ): self._handler_inicio_execucao_erro,
            (TransicaoCaso.ERRO_DADOS): self._handler_erro_dados,
            (
                TransicaoCaso.FLEXIBILIZACAO_SOLICITADA
            ): self._handler_flexibilizacao_solicitada,
            (TransicaoCaso.ERRO_MAX_FLEX): self._handler_erro_max_flex,
            (
                TransicaoCaso.FLEXIBILIZACAO_SUCESSO
            ): self._handler_flexibilizacao_sucesso,
            (
                TransicaoCaso.FLEXIBILIZACAO_ERRO
            ): self._handler_flexibilizacao_erro,
            (TransicaoCaso.CONCLUIDO): self._handler_caso_concluido,
            (TransicaoCaso.INVIAVEL): self._handler_caso_inviavel,
            (TransicaoCaso.ERRO): self._handler_erro,
            (
                TransicaoJob.SUBMISSAO_SOLICITADA
            ): self._handler_submissao_solicitada_job,
            (
                TransicaoJob.SUBMISSAO_SUCESSO
            ): self._handler_submissao_sucesso_job,
            (TransicaoJob.SUBMISSAO_ERRO): self._handler_submissao_erro_job,
            (TransicaoJob.INICIO_EXECUCAO): self._handler_inicio_execucao_job,
            (TransicaoJob.FIM_EXECUCAO): self._handler_fim_execucao_job,
            (TransicaoJob.TIMEOUT_EXECUCAO): self._handler_timeout_execucao,
            (
                TransicaoJob.DELECAO_SOLICITADA
            ): self._handler_delecao_solicitada,
            (TransicaoJob.DELECAO_ERRO): self._handler_delecao_erro,
            (TransicaoJob.DELECAO_SUCESSO): self._handler_delecao_sucesso,
        }

    @abstractmethod
    def inicializa(
        self,
        casos_anteriores: List[Caso],
    ):
        """
        Realiza a inicialização do caso. Isto é, a extração e
        renomeação de arquivos que podem ser necessários.
        """
        raise NotImplementedError()

    @abstractmethod
    def prepara(
        self,
        casos_anteriores: List[Caso],
        regras_operacao_reservatorios: List[RegraReservatorio],
    ):
        """
        Realiza a preparação dos arquivos para adequação às
        necessidades do estudo encadeado e o encadeamento das
        variáveis selecionadas.
        """
        raise NotImplementedError()

    def inicia_execucao(self):
        """
        Inicia o processo de execução de um caso após as etapas
        de inicialização e preparação.
        """
        self.callback_evento(TransicaoCaso.INICIO_EXECUCAO_SOLICITADA)

    def __submete(self):
        """
        Cria um novo Job para o caso e o submete à fila.
        """
        chdir(self._caso.caminho)
        self._job_atual = Job(
            DadosJob("", self.nome_job, self.caminho_job, 0.0, 0.0, 0.0, 0)
        )
        self._caso.adiciona_job(self._job_atual)
        self._monitor_job_atual = MonitorJob(self._job_atual)
        self._monitor_job_atual.observa(self.callback_evento)
        if self._monitor_job_atual.submete(self._caso.numero_processadores):
            chdir(Configuracoes().caminho_base_estudo)
            self.callback_evento(TransicaoCaso.INICIO_EXECUCAO_SUCESSO)
        else:
            chdir(Configuracoes().caminho_base_estudo)
            self.callback_evento(TransicaoCaso.INICIO_EXECUCAO_ERRO)

    def monitora(self):
        """
        Realiza o monitoramento do estado do caso e também do
        job associado.
        """
        chdir(self._caso.caminho)
        Log.log().debug("Monitorando - caso...")
        self._monitor_job_atual.monitora()
        if not self._armazenador.armazena_caso():
            Log.log().error(f"Erro ao armazenar caso {self._caso.nome}")
            chdir(Configuracoes().caminho_base_estudo)
            self.callback_evento(TransicaoCaso.ERRO)
        chdir(Configuracoes().caminho_base_estudo)

    def observa(self, f: Callable):
        self._transicao_caso.append(f)

    def _handler_criado(self):
        Log.log().info(f"Caso {self._caso.nome}: criado")
        self._caso.estado = EstadoCaso.INICIADO
        self._transicao_caso(TransicaoCaso.CRIADO)

    def _handler_prepara_execucao_solicitada(self):
        Log.log().info(f"Caso {self._caso.nome}: iniciando preparação do caso")
        self._caso.estado = EstadoCaso.PREPARANDO
        self._transicao_caso(TransicaoCaso.PREPARA_EXECUCAO_SOLICITADA)

    def _handler_prepara_execucao_sucesso(self):
        Log.log().info(f"Caso {self._caso.nome}: caso preparado com sucesso")
        self._caso.estado = EstadoCaso.PREPARADO
        self._transicao_caso(TransicaoCaso.PREPARA_EXECUCAO_SUCESSO)

    def _handler_prepara_execucao_erro(self):
        Log.log().info(f"Caso {self._caso.nome}: erro na preparação do caso")
        self._caso.estado = EstadoCaso.ERRO_PREPARACAO
        self.__armazena_caso()
        self.__sintetiza_caso()
        self.callback_evento(TransicaoCaso.ERRO)

    def _handler_inicio_execucao_solicitada(self):
        Log.log().info(f"Caso {self._caso.nome}: solicitada execução do caso")
        self._caso.estado = EstadoCaso.INICIANDO_EXECUCAO
        self._transicao_caso(TransicaoCaso.INICIO_EXECUCAO_SOLICITADA)
        self.__submete()

    def _handler_inicio_execucao_sucesso(self):
        Log.log().info(
            f"Caso {self._caso.nome}: início da execução com sucesso"
        )
        self._transicao_caso(TransicaoCaso.INICIO_EXECUCAO_SUCESSO)
        # Nada a fazer, visto que agora existe o job na fila e as transições
        # acontecem escutando os eventos do Job, até ser finalizado.

    def _handler_inicio_execucao_erro(self):
        Log.log().info(
            f"Caso {self._caso.nome}: erro no início da execução do caso"
        )
        self._caso.estado = EstadoCaso.ERRO_EXECUCAO
        self.__armazena_caso()
        self.__sintetiza_caso()
        self.callback_evento(TransicaoCaso.ERRO)

    def _handler_submissao_solicitada_job(self):
        Log.log().info(
            f"Caso {self._caso.nome}: submissão do job do caso solicitada"
        )

    def _handler_submissao_sucesso_job(self):
        Log.log().info(f"Caso {self._caso.nome}: sucesso na submissão do caso")
        self._caso.estado = EstadoCaso.ESPERANDO_FILA

    def _handler_submissao_erro_job(self):
        Log.log().info(
            f"Caso {self._caso.nome}: erro na submissão do job do caso"
        )
        self._caso.estado = EstadoCaso.ERRO_COMUNICACAO
        self.__armazena_caso()
        self.__sintetiza_caso()
        self.callback_evento(TransicaoCaso.ERRO)

    def _handler_inicio_execucao_job(self):
        Log.log().info(f"Caso {self._caso.nome}: iniciou execução")
        self._caso.estado = EstadoCaso.EXECUTANDO

    def _handler_delecao_solicitada(self):
        # Aguarda o job ser deletado completamente
        Log.log().debug(f"Caso {self._caso.nome}: deleção solicitada")

    def _handler_delecao_erro(self):
        Log.log().info(
            f"Caso {self._caso.nome}: erro na deleção de um caso com timeout"
        )
        self._caso.estado = EstadoCaso.ERRO_COMUNICACAO
        self.__armazena_caso()
        self.__sintetiza_caso()
        self.callback_evento(TransicaoCaso.ERRO)

    def _handler_delecao_sucesso(self):
        Log.log().debug(f"Caso {self._caso.nome}: caso com timeout deletado")
        self.callback_evento(TransicaoCaso.INICIO_EXECUCAO_SOLICITADA)

    def _handler_erro_dados(self):
        Log.log().info(f"Caso {self._caso.nome}: erro de dados")
        self._caso.estado = EstadoCaso.ERRO_DADOS
        self.__armazena_caso()
        self.__sintetiza_caso()
        self.callback_evento(TransicaoCaso.ERRO)

    def _handler_erro_max_flex(self):
        Log.log().info(
            f"Caso {self._caso.nome}: máximo de flexibilizações atingido"
        )
        self._caso.estado = EstadoCaso.ERRO_MAX_FLEX
        self.__armazena_caso()
        self.__sintetiza_caso()
        self.callback_evento(TransicaoCaso.ERRO)

    def _handler_timeout_execucao(self):
        Log.log().debug(f"Caso {self._caso.nome}: timeout durante a execução")
        self._monitor_job_atual.deleta()

    @abstractmethod
    def _handler_fim_execucao_job(self):
        raise NotImplementedError()

    @abstractmethod
    def _handler_caso_inviavel(self):
        raise NotImplementedError()

    @abstractmethod
    def _handler_flexibilizacao_solicitada(self):
        raise NotImplementedError()

    @abstractmethod
    def _handler_flexibilizacao_sucesso(self):
        raise NotImplementedError()

    @abstractmethod
    def _handler_flexibilizacao_erro(self):
        raise NotImplementedError()

    def _handler_caso_concluido(self):
        Log.log().info(f"Caso {self._caso.nome}: caso concluído.")
        self._caso.estado = EstadoCaso.CONCLUIDO
        self.__armazena_caso()
        self.__sintetiza_caso()
        self._transicao_caso(TransicaoCaso.CONCLUIDO)

    def _handler_erro(self):
        Log.log().error(f"Caso {self._caso.nome}: Erro. ")
        self._transicao_caso(TransicaoCaso.ERRO)

    def __armazena_caso(self):
        if not self._armazenador.armazena_caso():
            Log.log().error(
                f"Caso {self._caso.nome}: Erro no armazenamento do caso - "
                + f"Estado: {self._caso.estado}."
            )
            self.callback_evento(TransicaoCaso.ERRO)

    def __sintetiza_caso(self):
        if not self._sintetizador.sintetiza_caso():
            Log.log().error(
                f"Caso {self._caso.nome}: Erro na síntese do caso - "
                + f"Estado: {self._caso.estado}."
            )
            self.callback_evento(TransicaoCaso.ERRO)

    @property
    def caso(self) -> Caso:
        return self._caso


class MonitorNEWAVE(MonitorCaso):
    def __init__(self, caso: CasoNEWAVE):
        super().__init__(caso)

    @property
    def caso(self) -> CasoNEWAVE:
        if not isinstance(self._caso, CasoNEWAVE):
            raise ValueError("MonitorNEWAVE tem um caso não de NEWAVE")
        return self._caso

    # Override
    @property
    def caminho_job(self) -> str:
        dir_base = Configuracoes().diretorio_instalacao_newaves
        versao = Configuracoes().versao_newave
        dir_versao = join(dir_base, versao)
        arquivos_versao = listdir(dir_versao)
        arq_job = [a for a in arquivos_versao if ".job" in a]
        return join(dir_versao, arq_job[0])

    # Override
    @property
    def nome_job(self) -> str:
        return f"NW{self.caso.ano}{self.caso.mes}"

    # Override
    def inicializa(self, casos_anteriores: List[Caso]):
        """
        Realiza a inicialização do caso. Isto é, a extração e
        renomeação de arquivos que podem ser necessários.
        """
        # Se não é o primeiro NEWAVE, apaga os cortes do último
        ultimo_nw = None
        for c in reversed(casos_anteriores):
            if isinstance(c, CasoNEWAVE):
                ultimo_nw = c
                break
        if ultimo_nw is not None:
            sint_ultimo = SintetizadorNEWAVE(ultimo_nw)
            if sint_ultimo.verifica_cortes_extraidos():
                sint_ultimo.deleta_cortes()

        self.callback_evento(TransicaoCaso.CRIADO)

    # Override
    def prepara(
        self,
        casos_anteriores: List[Caso],
        regras_operacao_reservatorios: List[RegraReservatorio],
    ):
        """
        Realiza a preparação dos arquivos para adequação às
        necessidades do estudo encadeado e o encadeamento das
        variáveis selecionadas.
        """
        self.callback_evento(TransicaoCaso.PREPARA_EXECUCAO_SOLICITADA)
        preparador = PreparadorCaso.factory(self._caso)
        sucesso_prepara = preparador.prepara_caso()
        sucesso_encadeia = preparador.encadeia_variaveis(casos_anteriores)
        sucesso_regras = preparador.aplica_regras_operacao_reservatorios(
            casos_anteriores, regras_operacao_reservatorios
        )
        sucessos = all([sucesso_prepara, sucesso_encadeia, sucesso_regras])
        if sucessos:
            self.callback_evento(TransicaoCaso.PREPARA_EXECUCAO_SUCESSO)
        else:
            self.callback_evento(TransicaoCaso.PREPARA_EXECUCAO_ERRO)

    # Override
    def _handler_fim_execucao_job(self):
        Log.log().info(f"Caso {self._caso.nome}: fim da execução")
        if not self._avaliador.avalia():
            self.callback_evento(TransicaoCaso.ERRO_DADOS)
        else:
            self.callback_evento(TransicaoCaso.CONCLUIDO)

    def _handler_caso_inviavel(self):
        raise NotImplementedError()

    def _handler_flexibilizacao_solicitada(self):
        raise NotImplementedError()

    def _handler_flexibilizacao_sucesso(self):
        raise NotImplementedError()

    def _handler_flexibilizacao_erro(self):
        raise NotImplementedError()


class MonitorDECOMP(MonitorCaso):
    def __init__(self, caso: CasoDECOMP):
        super().__init__(caso)

    @property
    def caso(self) -> CasoDECOMP:
        if not isinstance(self._caso, CasoDECOMP):
            raise ValueError("MonitorDECOMP tem um caso não de DECOMP")
        return self._caso

    # Override
    @property
    def caminho_job(self) -> str:
        dir_base = Configuracoes().diretorio_instalacao_decomps
        versao = Configuracoes().versao_decomp
        dir_versao = join(dir_base, versao)
        arquivos_versao = listdir(dir_versao)
        arq_job = [a for a in arquivos_versao if ".job" in a]
        return join(dir_versao, arq_job[0])

    # Override
    @property
    def nome_job(self) -> str:
        return f"DC{self.caso.ano}{self.caso.mes}{self.caso.revisao}"

    # Override
    def inicializa(
        self,
        casos_anteriores: List[Caso],
    ):
        """
        Realiza a inicialização do caso. Isto é, a extração e
        renomeação de arquivos que podem ser necessários.
        """
        self.callback_evento(TransicaoCaso.CRIADO)

    # Override
    def prepara(
        self,
        casos_anteriores: List[Caso],
        regras_operacao_reservatorios: List[RegraReservatorio],
    ):
        """
        Realiza a preparação dos arquivos para adequação às
        necessidades do estudo encadeado e o encadeamento das
        variáveis selecionadas.
        """
        self.callback_evento(TransicaoCaso.PREPARA_EXECUCAO_SOLICITADA)
        preparador = PreparadorCaso.factory(self._caso)
        ultimo_newave = next(
            c
            for c in reversed(casos_anteriores)
            if (isinstance(c, CasoNEWAVE) and c.estado == EstadoCaso.CONCLUIDO)
        )
        sucesso_prepara = preparador.prepara_caso(caso_cortes=ultimo_newave)
        sucesso_encadeia = preparador.encadeia_variaveis(casos_anteriores)
        sucesso_regras = preparador.aplica_regras_operacao_reservatorios(
            casos_anteriores, regras_operacao_reservatorios
        )
        if all([sucesso_prepara, sucesso_encadeia, sucesso_regras]):
            self.callback_evento(TransicaoCaso.PREPARA_EXECUCAO_SUCESSO)
        else:
            self.callback_evento(TransicaoCaso.PREPARA_EXECUCAO_ERRO)

    # Override
    def _handler_fim_execucao_job(self):
        Log.log().info(f"Caso {self._caso.nome}: fim da execução")
        if not self._avaliador.avalia():
            self.callback_evento(TransicaoCaso.INVIAVEL)
        else:
            self.callback_evento(TransicaoCaso.CONCLUIDO)

    # Override
    def _handler_caso_inviavel(self):
        n_flex = self._caso.numero_flexibilizacoes
        if n_flex >= Configuracoes().maximo_flexibilizacoes_revisao:
            self.callback_evento(TransicaoCaso.ERRO_MAX_FLEX)
        else:
            self.callback_evento(TransicaoCaso.FLEXIBILIZACAO_SOLICITADA)

    def _handler_flexibilizacao_solicitada(self):
        Log.log().info(f"Caso {self._caso.nome}: flexibilização solicitada")
        self._caso.estado = EstadoCaso.FLEXIBILIZANDO
        self.__flexibiliza()

    def _handler_flexibilizacao_sucesso(self):
        Log.log().info(
            f"Caso {self._caso.nome}: flexibilização realizada com sucesso."
        )
        self.callback_evento(TransicaoCaso.INICIO_EXECUCAO_SOLICITADA)

    def _handler_flexibilizacao_erro(self):
        Log.log().info(f"Caso {self._caso.nome}: erro na flexibilização.")
        self._caso.estado = EstadoCaso.ERRO
        self.callback_evento(TransicaoCaso.ERRO)

    # Override
    def __flexibiliza(self):
        flexibilizador = Flexibilizador.factory(self._caso)
        if not flexibilizador.flexibiliza():
            self.callback_evento(TransicaoCaso.FLEXIBILIZACAO_ERRO)
        else:
            self.callback_evento(TransicaoCaso.FLEXIBILIZACAO_SUCESSO)
