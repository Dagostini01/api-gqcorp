/**
 * Serviço de Transformação de Dados
 * Converte dados brutos dos robôs para o modelo relacional normalizado
 */

import { PrismaClient } from '@prisma/client';
import {
  BrasilRawData,
  PeruRawData,
  ChileRawData,
  ImportData,
  transformBrasilData,
  transformPeruData,
  transformChileData,
  INITIAL_COUNTRIES,
  INITIAL_STATES_BRASIL
} from './field-mapping';

export class DataTransformer {
  private prisma: PrismaClient;
  private countryCache = new Map<string, number>();
  private stateCache = new Map<string, number>();
  private productCache = new Map<string, number>();
  private companyCache = new Map<string, number>();

  constructor() {
    this.prisma = new PrismaClient();
  }

  /**
   * Inicializa dados básicos (países, estados)
   */
  async initializeBaseData(): Promise<void> {
    console.log('Inicializando dados básicos...');

    // Criar países
    for (const countryData of INITIAL_COUNTRIES) {
      const country = await this.prisma.country.upsert({
        where: { code: countryData.code },
        update: {},
        create: countryData,
      });
      this.countryCache.set(countryData.code, country.id);
    }

    // Criar estados do Brasil
    const brasilId = this.countryCache.get('BR');
    if (brasilId) {
      for (const stateData of INITIAL_STATES_BRASIL) {
        const state = await this.prisma.state.upsert({
          where: { 
            code_countryId: { 
              code: stateData.code, 
              countryId: brasilId 
            } 
          },
          update: {},
          create: {
            ...stateData,
            countryId: brasilId,
          },
        });
        this.stateCache.set(`${stateData.code}-${brasilId}`, state.id);
      }
    }

    console.log('Dados básicos inicializados com sucesso!');
  }

  /**
   * Processa dados do Brasil
   */
  async processBrasilData(rawDataArray: BrasilRawData[]): Promise<void> {
    console.log(`Processando ${rawDataArray.length} registros do Brasil...`);

    for (const rawData of rawDataArray) {
      try {
        // Transformar dados básicos
        const importData = transformBrasilData(rawData);

        // Resolver relacionamentos
        const countryId = await this.resolveCountry(rawData.country_code);
        const stateId = await this.resolveState(rawData.state, countryId);
        const productId = await this.resolveProduct(rawData.partida, rawData.descComer);
        const companyId = await this.resolveCompany(rawData.importador, rawData.importador, countryId);
        const originCountryId = await this.resolveOriginCountry(rawData.paisOrig);

        // Criar registro de importação
        await this.prisma.import.create({
          data: {
            ...importData,
            countryId,
            stateId,
            productId,
            companyId,
            originCountryId,
          } as any,
        });

      } catch (error) {
        console.error('Erro ao processar registro do Brasil:', error);
        console.error('Dados:', rawData);
      }
    }

    console.log('Processamento do Brasil concluído!');
  }

  /**
   * Processa dados do Peru
   */
  async processPeruData(rawDataArray: PeruRawData[]): Promise<void> {
    console.log(`Processando ${rawDataArray.length} registros do Peru...`);

    for (const rawData of rawDataArray) {
      try {
        // Transformar dados básicos
        const importData = transformPeruData(rawData);

        // Resolver relacionamentos
        const countryId = await this.resolveCountry(rawData.country_code);
        const stateId = await this.resolveState(rawData.state, countryId);
        const productId = await this.resolveProduct(rawData.partida);
        const companyId = await this.resolveCompany(rawData.importador, rawData.importador, countryId);

        // Criar registro de importação
        await this.prisma.import.create({
          data: {
            ...importData,
            countryId,
            stateId,
            productId,
            companyId,
          } as any,
        });

      } catch (error) {
        console.error('Erro ao processar registro do Peru:', error);
        console.error('Dados:', rawData);
      }
    }

    console.log('Processamento do Peru concluído!');
  }

  /**
   * Processa dados do Chile
   */
  async processChileData(rawDataArray: ChileRawData[]): Promise<void> {
    console.log(`Processando ${rawDataArray.length} registros do Chile...`);

    for (const rawData of rawDataArray) {
      try {
        // Transformar dados básicos
        const importData = transformChileData(rawData);

        // Resolver relacionamentos
        const countryId = await this.resolveCountry(rawData.country_code);
        const stateId = await this.resolveState(rawData.state, countryId);

        // Para Chile, precisaremos mapear campos específicos
        // Por enquanto, criar registro básico
        await this.prisma.import.create({
          data: {
            ...importData,
            countryId,
            stateId,
            declarationNumber: 'CHILE-' + Date.now(), // Temporário
            productId: await this.resolveProduct('UNKNOWN'),
            companyId: await this.resolveCompany('UNKNOWN', 'UNKNOWN', countryId),
          } as any,
        });

      } catch (error) {
        console.error('Erro ao processar registro do Chile:', error);
        console.error('Dados:', rawData);
      }
    }

    console.log('Processamento do Chile concluído!');
  }

  /**
   * Resolve país por código (com cache)
   */
  private async resolveCountry(countryCode: string): Promise<number> {
    if (this.countryCache.has(countryCode)) {
      return this.countryCache.get(countryCode)!;
    }

    const country = await this.prisma.country.findUnique({
      where: { code: countryCode }
    });

    if (!country) {
      throw new Error(`País não encontrado: ${countryCode}`);
    }

    this.countryCache.set(countryCode, country.id);
    return country.id;
  }

  /**
   * Resolve estado por código e país (com cache)
   */
  private async resolveState(stateCode: string, countryId: number): Promise<number | null> {
    const cacheKey = `${stateCode}-${countryId}`;
    
    if (this.stateCache.has(cacheKey)) {
      return this.stateCache.get(cacheKey)!;
    }

    const state = await this.prisma.state.findFirst({
      where: { code: stateCode, countryId }
    });

    if (state) {
      this.stateCache.set(cacheKey, state.id);
      return state.id;
    }

    return null;
  }

  /**
   * Resolve ou cria produto por código (com cache)
   */
  private async resolveProduct(productCode: string, description?: string): Promise<number> {
    if (this.productCache.has(productCode)) {
      return this.productCache.get(productCode)!;
    }

    let product = await this.prisma.product.findUnique({
      where: { code: productCode }
    });

    if (!product) {
      product = await this.prisma.product.create({
        data: {
          code: productCode,
          description: description || productCode,
          commercialDesc: description,
        }
      });
    }

    this.productCache.set(productCode, product.id);
    return product.id;
  }

  /**
   * Resolve ou cria empresa por documento e país (com cache)
   */
  private async resolveCompany(document: string, name: string, countryId: number): Promise<number> {
    const cacheKey = `${document}-${countryId}`;
    
    if (this.companyCache.has(cacheKey)) {
      return this.companyCache.get(cacheKey)!;
    }

    let company = await this.prisma.company.findFirst({
      where: { document, countryId }
    });

    if (!company) {
      company = await this.prisma.company.create({
        data: {
          document,
          name,
          countryId,
          type: 'IMPORTER',
        }
      });
    }

    this.companyCache.set(cacheKey, company.id);
    return company.id;
  }

  /**
   * Resolve país de origem por nome/código
   */
  private async resolveOriginCountry(originCountry: string): Promise<number | null> {
    if (!originCountry) return null;

    // Mapeamento de nomes para códigos
    const countryMapping: { [key: string]: string } = {
      'CHINA': 'CN',
      'ESTADOS UNIDOS': 'US',
      'ALEMANHA': 'DE',
      'JAPAO': 'JP',
      'COREIA DO SUL': 'KR',
      // Adicionar mais mapeamentos conforme necessário
    };

    const countryCode = countryMapping[originCountry.toUpperCase()] || originCountry;

    try {
      return await this.resolveCountry(countryCode);
    } catch {
      // Se não encontrar, criar país genérico
      const country = await this.prisma.country.upsert({
        where: { code: countryCode.substring(0, 2).toUpperCase() },
        update: {},
        create: {
          code: countryCode.substring(0, 2).toUpperCase(),
          name: originCountry,
          fullName: originCountry,
        }
      });
      return country.id;
    }
  }

  /**
   * Processa dados de qualquer país baseado no JSON de resposta dos robôs
   */
  async processRobotResponse(jsonResponse: any): Promise<void> {
    const { descricao, total, resultados } = jsonResponse;

    console.log(`Processando resposta: ${descricao}`);
    console.log(`Total de registros: ${total}`);

    if (!resultados || !Array.isArray(resultados)) {
      throw new Error('Formato de resposta inválido');
    }

    // Detectar país baseado no primeiro registro
    const firstRecord = resultados[0];
    if (!firstRecord || !firstRecord.country_code) {
      throw new Error('Não foi possível detectar o país dos dados');
    }

    const countryCode = firstRecord.country_code;

    // Processar baseado no país
    switch (countryCode) {
      case 'BR':
        await this.processBrasilData(resultados as BrasilRawData[]);
        break;
      case 'PE':
        await this.processPeruData(resultados as PeruRawData[]);
        break;
      case 'CL':
        await this.processChileData(resultados as ChileRawData[]);
        break;
      default:
        throw new Error(`País não suportado: ${countryCode}`);
    }

    // Registrar execução da consulta
    await this.prisma.queryExecution.create({
      data: {
        countryCode,
        queryType: 'IMPORT',
        parameters: { descricao },
        totalRecords: total,
        executionTime: 0, // Será calculado pela API
        status: 'SUCCESS',
      }
    });
  }

  /**
   * Limpa caches
   */
  clearCaches(): void {
    this.countryCache.clear();
    this.stateCache.clear();
    this.productCache.clear();
    this.companyCache.clear();
  }

  /**
   * Fecha conexão com o banco
   */
  async disconnect(): Promise<void> {
    await this.prisma.$disconnect();
  }
}