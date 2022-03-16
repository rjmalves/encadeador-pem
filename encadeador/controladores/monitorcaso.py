from abc import abstractmethod
from typing import Dict, List, Tuple, Callable
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

    def callback_evento_job(self, evento: TransicaoJob):
        """
        Esta função é usada para implementar o Observer Pattern.
        Quando chamada, significa que ocorreu algo com o job do caso
        em um GerenciadorFila e deve reagir atualizando os campos
        adequados nos objetos.

        :param evento: O evento ocorrido com o job
        :type evento: TransicaoJob
        """
        # Executa a ação da transição de estado
        novo_estado = self._regras()[self._caso.estado, evento]()
        # Atualiza o estado atual
        self._caso.atualiza(novo_estado)

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
    ) -> Dict[Tuple[EstadoCaso, TransicaoJob], None]:
        return {
            (
                EstadoCaso.INICIADO,
                TransicaoJob.ENTRADA_FILA,
            ): self._trata_entrada_fila,
            (
                EstadoCaso.ESPERANDO_FILA,
                TransicaoJob.INICIO_EXECUCAO,
            ): self._trata_inicio_execucao,
            (
                EstadoCaso.ESPERANDO_FILA,
                TransicaoJob.COMANDO_DELETA_JOB,
            ): self._trata_inicio_del_job,
            (
                EstadoCaso.EXECUTANDO,
                TransicaoJob.COMANDO_DELETA_JOB,
            ): self._trata_inicio_del_job,
            (
                EstadoCaso.EXECUTANDO,
                TransicaoJob.ERRO_EXECUCAO,
            ): self._trata_erro_execucao,
            (
                EstadoCaso.EXECUTANDO,
                TransicaoJob.FIM_EXECUCAO,
            ): self._trata_fim_execucao,
            (
                EstadoCaso.ESPERANDO_DEL_JOB,
                TransicaoJob.FIM_EXECUCAO,
            ): self._trata_erro,
        }

    @abstractmethod
    def inicializa(self, casos_anteriores: List[Caso]) -> bool:
        """
        Realiza a inicialização do caso, isto é, a preparação dos
        arquivos para adequação às necessidades do estudo
        encadeado e o encadeamento das variáveis selecionadas.

        :return: O sucesso ou não da inicialização do caso.
        :rtype: bool
        """
        raise NotImplementedError()

    def submete(self, retry: bool = False) -> bool:
        """
        Cria um novo Job para o caso e o submete à fila.

        :return: O sucesso ou não da submissão do job.
        :rtype: bool
        """
        chdir(self._caso.caminho)
        self._job_atual = Job(
            DadosJob("", self.nome_job, self.caminho_job, 0.0, 0.0, 0.0, 0)
        )
        self._caso.adiciona_job(self._job_atual, retry)
        self._monitor_job_atual = MonitorJob(self._job_atual)
        self._monitor_job_atual.observa(self.callback_evento_job)
        ret = self._monitor_job_atual.submete(self._caso.numero_processadores)
        Log.log().info(f"Caso {self._caso.nome}: submetido")
        chdir(Configuracoes().caminho_base_estudo)
        return ret

    def monitora(self):
        """
        Realiza o monitoramento do estado do caso e também do
        job associado.
        """
        chdir(self._caso.caminho)
        # Trata os casos onde não existe job: inviável e flexibilizando
        if self._caso.estado == EstadoCaso.INVIAVEL:
            self._trata_caso_inviavel()
        elif self._caso.estado == EstadoCaso.FLEXIBILIZANDO:
            self._trata_caso_flexibilizando()
        # Trata os casos com job
        self._monitor_job_atual.monitora()
        if not self._armazenador.armazena_caso():
            Log.log().error(f"Erro ao armazenar caso {self._caso.nome}")
        chdir(Configuracoes().caminho_base_estudo)

    def observa(self, f: Callable):
        self._transicao_caso.append(f)

    def _trata_entrada_fila(self):
        Log.log().info(f"Caso {self._caso.nome}: esperando na fila")
        self._caso.atualiza(EstadoCaso.ESPERANDO_FILA)

    def _trata_inicio_execucao(self):
        Log.log().info(f"Caso {self._caso.nome}: iniciou execução")
        self._caso.atualiza(EstadoCaso.EXECUTANDO)

    def _trata_inicio_del_job(self):
        # Aguarda o job ser deletado completamente
        Log.log().info(f"Caso {self._caso.nome}: deleção solicitada")
        self._caso.atualiza(EstadoCaso.ESPERANDO_DEL_JOB)

    def _trata_erro_execucao(self):
        Log.log().info(f"Caso {self._caso.nome}: erro durante a execução")
        self._caso.atualiza(EstadoCaso.ESPERANDO_DEL_JOB)

    @abstractmethod
    def _trata_fim_execucao(self):
        raise NotImplementedError()

    @abstractmethod
    def _trata_caso_inviavel(self):
        raise NotImplementedError()

    @abstractmethod
    def _trata_caso_flexibilizando(self):
        raise NotImplementedError()

    def _trata_erro(self):
        Log.log().info(
            f"Caso {self._caso.nome}: erro de comunicação. "
            + "Submetendo novamente (retry)..."
        )
        if not self.submete(True):
            Log.log().info(f"Caso {self._caso.nome}: erro durante o retry")
            self._transicao_caso(TransicaoCaso.ERRO)
            self._caso.atualiza(EstadoCaso.ERRO)
            raise RuntimeError()
        self._caso.atualiza(EstadoCaso.ESPERANDO_FILA)

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
    def inicializa(self, casos_anteriores: List[Caso]) -> bool:
        """
        Realiza a inicialização do caso, isto é, a preparação dos
        arquivos para adequação às necessidades do estudo
        encadeado e o encadeamento das variáveis selecionadas.

        :return: O sucesso ou não da inicialização do caso.
        :rtype: bool
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

        self._caso.atualiza(EstadoCaso.INICIADO)
        self._transicao_caso(TransicaoCaso.INICIOU)
        preparador = PreparadorCaso.factory(self._caso)
        sucesso_prepara = preparador.prepara_caso()
        sucesso_encadeia = preparador.encadeia_variaveis(casos_anteriores)
        return sucesso_prepara and sucesso_encadeia

    # Override
    def _trata_fim_execucao(self):
        Log.log().info(f"Caso {self._caso.nome}: fim da execução")
        if not self._avaliador.avalia():
            Log.log().info(f"Caso {self._caso.nome}: erro de dados")
            self._transicao_caso(TransicaoCaso.ERRO_DADOS)
            self._caso.atualiza(EstadoCaso.ERRO_DADOS)
            return
        self._caso.atualiza(EstadoCaso.CONCLUIDO)
        sintetizador = SintetizadorCaso.factory(self._caso)
        if not sintetizador.sintetiza_caso():
            Log.log().error(f"Erro na síntese do caso {self._caso.nome}")
        self._transicao_caso(TransicaoCaso.SUCESSO)

    # Override
    def _trata_caso_inviavel(self):
        raise NotImplementedError()

    # Override
    def _trata_caso_flexibilizando(self):
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
    def inicializa(self, casos_anteriores: List[Caso]) -> bool:
        """
        Realiza a inicialização do caso, isto é, a preparação dos
        arquivos para adequação às necessidades do estudo
        encadeado e o encadeamento das variáveis selecionadas.

        :return: O sucesso ou não da inicialização do caso.
        :rtype: bool
        """
        self._caso.atualiza(EstadoCaso.INICIADO)
        self._transicao_caso(TransicaoCaso.INICIOU)
        preparador = PreparadorCaso.factory(self._caso)
        ultimo_newave = next(
            c
            for c in reversed(casos_anteriores)
            if (isinstance(c, CasoNEWAVE) and c.estado == EstadoCaso.CONCLUIDO)
        )
        sucesso_prepara = preparador.prepara_caso(caso_cortes=ultimo_newave)
        sucesso_encadeia = preparador.encadeia_variaveis(casos_anteriores)
        return sucesso_prepara and sucesso_encadeia

    # Override
    def _trata_fim_execucao(self):
        Log.log().info(f"Caso {self._caso.nome}: fim da execução")
        if not self._avaliador.avalia():
            self._caso.atualiza(EstadoCaso.INVIAVEL)
            return

        self._caso.atualiza(EstadoCaso.CONCLUIDO)
        sintetizador = SintetizadorCaso.factory(self._caso)
        if not sintetizador.sintetiza_caso():
            Log.log().error(f"Erro na síntese do caso {self._caso.nome}")
        self._transicao_caso(TransicaoCaso.SUCESSO)

    # Override
    def _trata_caso_inviavel(self):
        n_flex = self._caso.numero_flexibilizacoes
        if n_flex >= Configuracoes().maximo_flexibilizacoes_revisao:
            Log.log().info(
                f"Caso {self._caso.nome}: máximo de "
                + "flexibilizações atingido"
            )
            self._transicao_caso(TransicaoCaso.ERRO_MAX_FLEX)
            self._caso.atualiza(EstadoCaso.ERRO_MAX_FLEX)
        else:
            self._caso.atualiza(EstadoCaso.FLEXIBILIZANDO)

    # Override
    def _trata_caso_flexibilizando(self):
        flexibilizador = Flexibilizador.factory(self._caso)
        self._caso.atualiza(EstadoCaso.INICIADO)
        if not flexibilizador.flexibiliza():
            Log.log().error(f"Caso {self._caso.nome}: erro na flexibilização")
            self._caso.atualiza(EstadoCaso.ERRO)
            self._transicao_caso(TransicaoCaso.ERRO)
            raise RuntimeError()
        if not self.submete():
            Log.log().error(
                f"Caso {self._caso.nome}: erro " + "na submissão do job do caso"
            )
            self._caso.atualiza(EstadoCaso.ERRO)
            self._transicao_caso(TransicaoCaso.ERRO)
            raise RuntimeError()
