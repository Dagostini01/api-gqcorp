/**
 * Mapeamento de campos brutos dos JSONs dos robôs para o modelo normalizado
 * API GQCorp - Brasil, Peru, Chile
 */

// ===== INTERFACES PARA DADOS BRUTOS =====

export interface BrasilRawData {
  country_code: string;
  state: string;
  // Série enviada pelo robô (ex.: "BR")
  serie?: string;
  ano_ref: string;
  mes_ref: string;
  importador: string;
  declaracao: string;
  partida: string;
  fecNumeracao: string;
  paisOrig: string;
  descComer: string;
  fobUsd: string;
  fleteUsd: string;
  seguro: string;
  cif: string;
  pesoNeto: string;
}

export interface PeruRawData {
  country_code: string;
  state: string;
  ano_ref: string;
  mes_ref: string;
  declaracao: string;
  importador: string;
  fobUsd: string;
  partida: string;
  // Campos específicos do Peru (baseado em CAMPOS)
  [key: string]: string;
}

export interface ChileRawData {
  country_code: string;
  state: string;
  ano_ref: string;
  mes_ref: string;
  // Campos específicos do Chile
  [key: string]: string;
}

// ===== MAPEAMENTO DE CAMPOS =====

export const FIELD_MAPPING = {
  // Campos comuns a todos os países
  COMMON: {
    country_code: 'countryId', // Será resolvido via lookup
    state: 'stateId', // Será resolvido via lookup
    ano_ref: 'operationDate', // Combinado com mes_ref
    mes_ref: 'operationDate', // Combinado com ano_ref
    declaracao: 'declarationNumber',
    importador: 'companyId', // Será resolvido via lookup
    partida: 'productId', // Será resolvido via lookup
    fobUsd: 'fobUsd',
  },

  // Campos específicos do Brasil
  BRASIL: {
    fecNumeracao: 'numerationDate',
    paisOrig: 'originCountryId', // Será resolvido via lookup
    descComer: 'commercialDesc', // Vai para Product.commercialDesc
    fleteUsd: 'freightUsd',
    seguro: 'insuranceUsd',
    cif: 'cifUsd',
    pesoNeto: 'netWeight',
  },

  // Campos específicos do Peru (baseado na análise do CAMPOS)
  PERU: {
    // Mapeamento será expandido conforme análise detalhada dos campos
    // Exemplo de campos identificados:
    // 'campo_especifico_peru': 'campo_normalizado'
  },

  // Campos específicos do Chile
  CHILE: {
    // Mapeamento será expandido conforme análise detalhada
    // 'campo_especifico_chile': 'campo_normalizado'
  }
};

// ===== FUNÇÕES DE TRANSFORMAÇÃO =====

/**
 * Converte dados brutos do Brasil para o modelo normalizado
 */
export function transformBrasilData(rawData: BrasilRawData): Partial<ImportData> {
  return {
    declarationNumber: rawData.declaracao,
    series: rawData.serie,
    numerationDate: rawData.fecNumeracao,
    fobUsd: parseDecimal(rawData.fobUsd),
    freightUsd: parseDecimal(rawData.fleteUsd),
    insuranceUsd: parseDecimal(rawData.seguro),
    cifUsd: parseDecimal(rawData.cif),
    netWeight: parseDecimal(rawData.pesoNeto),
    // Alguns retornos do Comex não trazem ano_ref/mes_ref; derivamos do fecNumeracao (01/mm/yyyy)
    operationDate: parseDateFromFecNumeracao(rawData.fecNumeracao) || parseDate(rawData.ano_ref, rawData.mes_ref),
    dataSource: 'COMEXSTAT',
  };
}

/**
 * Converte dados brutos do Peru para o modelo normalizado
 */
export function transformPeruData(rawData: PeruRawData): Partial<ImportData> {
  // Valores brutos potenciais (variações de nomes)
  const fecRaw = (rawData as any).fecNumeracao || (rawData as any).fecNumeracion || (rawData as any).fecNumerac || (rawData as any).fec;
  const seriesRaw = (rawData as any).series ?? (rawData as any).serie;
  let declaracaoRaw = (rawData as any).declaracao;

  // Heurística: algumas linhas vêm com cabeçalho na coluna "declaracao" e valores deslocados
  const headerRegex = /Declaraci[oó]n\s+Importador\s+Fec\.\s+Numeraci[oó]n\s+Agencia\s+Ser/i;
  const looksLikeHeader = typeof declaracaoRaw === 'string' && headerRegex.test(declaracaoRaw);

  // Se parecer cabeçalho, realinha: usa fecRaw como declaração se tiver formato de DUI, e série como data
  const duiRegex = /^\d{3}-\d{2}-\d{6}$/; // Ex.: 118-25-000880
  let declarationNumberFinal: string | undefined;
  let numerationDateFromSeries: string | undefined;
  let paisOrigRealigned: string | undefined;
  let paisAdqRealigned: string | undefined;
  
  if (looksLikeHeader) {
    if (typeof fecRaw === 'string' && duiRegex.test(fecRaw)) {
      declarationNumberFinal = fecRaw;
    }
    if (typeof seriesRaw === 'string') {
      const m = seriesRaw.match(/(\d{2}\/\d{2}\/\d{4})/);
      if (m) numerationDateFromSeries = m[1];
    }
    
    // Nos registros com cabeçalho deslocado, paisOrig e paisAdq também estão trocados
    // Tentar encontrar códigos de país válidos nos campos corretos
    const possibleCountries = [
      (rawData as any).paisOrig, (rawData as any).paisAdq, 
      (rawData as any).paisOrigName, (rawData as any).paisAdqName
    ];
    
    for (const field of possibleCountries) {
      if (typeof field === 'string') {
        // Procurar por códigos de país de 2-3 letras ou números válidos
        const countryMatch = field.match(/^[A-Z]{2,3}$|^\d{1,3}$/);
        if (countryMatch && !paisOrigRealigned) {
          paisOrigRealigned = field;
        } else if (countryMatch && !paisAdqRealigned) {
          paisAdqRealigned = field;
        }
      }
    }
  }

  // Extrair data de numeração com regex dd/mm/yyyy de vários campos possíveis
  const fecMatch = typeof fecRaw === 'string' ? fecRaw.match(/(\d{2}\/\d{2}\/\d{4})/) : null;
  const numerationDate = numerationDateFromSeries || (fecMatch ? fecMatch[1] : undefined);

  // Série: normaliza e limita (se a série for na verdade uma data, não usar como série)
  let seriesSan = typeof seriesRaw === 'string' ? seriesRaw.trim() : undefined;
  if (seriesSan && /^(\d{2}\/\d{2}\/\d{4})$/.test(seriesSan)) {
    // Isso é uma data, então deixa somente em numerationDate
    seriesSan = undefined;
  }
  if (seriesSan) {
    // Mantém apenas alfanuméricos e limita a 20
    seriesSan = seriesSan.replace(/[^A-Za-z0-9]/g, '').slice(0, 20) || undefined;
  }
  if (!seriesSan && typeof (rawData as any).declaracao === 'string' && !looksLikeHeader) {
    // Fallback: tenta extrair token alfanumérico curto da declaração se não for cabeçalho
    const m = (rawData as any).declaracao.match(/[A-Za-z0-9]{1,20}/);
    seriesSan = m ? m[0] : undefined;
  }

  // Se numerationDate ainda não foi possível, tenta varrer outras propriedades do raw
  let numerationDateFinal = numerationDate;
  if (!numerationDateFinal) {
    for (const key of Object.keys(rawData)) {
      const val = (rawData as any)[key];
      if (typeof val === 'string') {
        const mm = val.match(/(\d{2}\/\d{2}\/\d{4})/);
        if (mm) { numerationDateFinal = mm[1]; break; }
      }
    }
  }

  // Declaração: usa realinhamento se aplicável, senão o valor bruto
  const declarationNumber = declarationNumberFinal || declaracaoRaw;

  // Países: usa realinhamento se aplicável, senão os valores brutos
  const originCountryCode = paisOrigRealigned || (rawData as any).paisOrig;
  const acquisitionCountryCode = paisAdqRealigned || (rawData as any).paisAdq;

  return {
    declarationNumber,
    series: seriesSan,
    numerationDate: numerationDateFinal,
    // Prefere ano_ref/mes_ref para Peru, cai para data extraída da numeração
    operationDate: parseDate((rawData as any).ano_ref, (rawData as any).mes_ref) || parseDateFromFecNumeracao(numerationDateFinal),
    // Países de origem e aquisição (serão resolvidos no data-transformer)
    originCountryCode,
    acquisitionCountryCode,
    // Valores monetários
    fobUsd: parseDecimal((rawData as any).fobUsd ?? (rawData as any).fob),
    freightUsd: parseDecimal((rawData as any).fleteUsd ?? (rawData as any).flete),
    insuranceUsd: parseDecimal((rawData as any).seguro ?? (rawData as any).seguro2),
    cifUsd: parseDecimal((rawData as any).cif),
    adValorem: parseDecimal((rawData as any).adv),
    igv: parseDecimal((rawData as any).igv),
    isc: parseDecimal((rawData as any).isc),
    ipm: parseDecimal((rawData as any).ipm),
    specialRights: parseDecimal((rawData as any).derEsp),
    previousRights: parseDecimal((rawData as any).derAnt),
    additionalIpm: parseDecimal((rawData as any).ipmAdic),
    // Pesos e quantidades
    netWeight: parseDecimal((rawData as any).pesoNeto),
    netWeight2: parseDecimal((rawData as any).pesoNeto2),
    quantity: parseDecimal((rawData as any).quantidade),
    packages: parseDecimal((rawData as any).nroBultos),
    unit: (rawData as any).unid,
    // Descrições e outros
    presentationDesc: (rawData as any).descPresent,
    materialDesc: (rawData as any).descMatConst,
    useDesc: (rawData as any).descUso,
    othersDesc: (rawData as any).descOutros,
    channel: (rawData as any).canal,
    warehouse: (rawData as any).armazen,
    commodity: (rawData as any).commod,
    dataSource: 'ADUANET',
  };
}

/**
 * Converte dados brutos do Chile para o modelo normalizado
 */
export function transformChileData(rawData: ChileRawData): Partial<ImportData> {
  return {
    operationDate: parseDate(rawData.ano_ref, rawData.mes_ref),
    dataSource: 'CKAN_CHILE',
  };
}

// ===== FUNÇÕES DE LOOKUP E RESOLUÇÃO =====

/**
 * Resolve país por código
 */
export async function resolveCountry(countryCode: string): Promise<number> {
  // Implementação com Prisma Client
  // const country = await prisma.country.findUnique({ where: { code: countryCode } });
  // return country?.id || 0;
  return 0; // Placeholder
}

/**
 * Resolve estado por código e país
 */
export async function resolveState(stateCode: string, countryId: number): Promise<number | null> {
  // Implementação com Prisma Client
  // const state = await prisma.state.findFirst({ 
  //   where: { code: stateCode, countryId } 
  // });
  // return state?.id || null;
  return null; // Placeholder
}

/**
 * Resolve ou cria produto por código
 */
export async function resolveProduct(productCode: string, description?: string): Promise<number> {
  // Implementação com Prisma Client
  // let product = await prisma.product.findUnique({ where: { code: productCode } });
  // if (!product) {
  //   product = await prisma.product.create({
  //     data: { code: productCode, description: description || productCode }
  //   });
  // }
  // return product.id;
  return 0; // Placeholder
}

/**
 * Resolve ou cria empresa por documento e país
 */
export async function resolveCompany(
  document: string, 
  name: string, 
  countryId: number
): Promise<number> {
  // Implementação com Prisma Client
  // let company = await prisma.company.findFirst({ 
  //   where: { document, countryId } 
  // });
  // if (!company) {
  //   company = await prisma.company.create({
  //     data: { document, name, countryId }
  //   });
  // }
  // return company.id;
  return 0; // Placeholder
}

// ===== FUNÇÕES UTILITÁRIAS =====

/**
 * Converte valor para número decimal (aceita string ou number)
 */
function parseDecimal(value: string | number): number | undefined {
  if (value === null || value === undefined) return undefined;
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : undefined;
  }
  const v = value.trim();
  if (v === '') return undefined;
  const parsed = parseFloat(v.replace(',', '.'));
  return isNaN(parsed) ? undefined : parsed;
}

/**
 * Converte ano e mês para Date
 */
function parseDate(year: string, month: string): Date | undefined {
  if (!year || !month) return undefined;
  const y = parseInt(year);
  const m = parseInt(month);
  if (isNaN(y) || isNaN(m) || m < 1 || m > 12) return undefined;
  return new Date(y, m - 1, 1); // Primeiro dia do mês
}

/**
 * Converte "01/mm/yyyy" para Date (primeiro dia do mês)
 */
function parseDateFromFecNumeracao(fec: string | undefined): Date | undefined {
  if (!fec) return undefined;
  const s = fec.trim();
  const m = s.match(/^\s*(\d{2})\/(\d{2})\/(\d{4})\s*$/);
  if (!m) return undefined;
  const dd = parseInt(m[1], 10);
  const mm = parseInt(m[2], 10);
  const yyyy = parseInt(m[3], 10);
  if (isNaN(dd) || isNaN(mm) || isNaN(yyyy) || mm < 1 || mm > 12) return undefined;
  return new Date(yyyy, mm - 1, 1);
}

// ===== INTERFACE PARA DADOS NORMALIZADOS =====

export interface ImportData {
  declarationNumber: string;
  series?: string;
  operationDate?: Date;
  numerationDate?: string;
  countryId: number;
  stateId?: number;
  productId: number;
  companyId: number;
  agencyId?: number;
  originCountryId?: number;
  acquisitionCountryId?: number;
  fobUsd?: number;
  freightUsd?: number;
  insuranceUsd?: number;
  cifUsd?: number;
  fobLocal?: number;
  freightLocal?: number;
  insuranceLocal?: number;
  adValorem?: number;
  igv?: number;
  isc?: number;
  ipm?: number;
  specialRights?: number;
  previousRights?: number;
  additionalIpm?: number;
  netWeight?: number;
  netWeight2?: number;
  quantity?: number;
  packages?: number;
  unit?: string;
  presentationDesc?: string;
  materialDesc?: string;
  useDesc?: string;
  othersDesc?: string;
  channel?: string;
  warehouse?: string;
  commodity?: string;
  dataSource: 'COMEXSTAT' | 'ADUANET' | 'CKAN_CHILE';
  rawData?: any;
}

// ===== DADOS INICIAIS PARA SEED =====

export const INITIAL_COUNTRIES = [
  { code: 'BR', name: 'Brasil', fullName: 'República Federativa do Brasil' },
  { code: 'PE', name: 'Peru', fullName: 'República del Perú' },
  { code: 'CL', name: 'Chile', fullName: 'República de Chile' },
];

export const INITIAL_STATES_BRASIL = [
  { code: 'AC', name: 'Acre' },
  { code: 'AL', name: 'Alagoas' },
  { code: 'AP', name: 'Amapá' },
  { code: 'AM', name: 'Amazonas' },
  { code: 'BA', name: 'Bahia' },
  { code: 'CE', name: 'Ceará' },
  { code: 'DF', name: 'Distrito Federal' },
  { code: 'ES', name: 'Espírito Santo' },
  { code: 'GO', name: 'Goiás' },
  { code: 'MA', name: 'Maranhão' },
  { code: 'MT', name: 'Mato Grosso' },
  { code: 'MS', name: 'Mato Grosso do Sul' },
  { code: 'MG', name: 'Minas Gerais' },
  { code: 'PA', name: 'Pará' },
  { code: 'PB', name: 'Paraíba' },
  { code: 'PR', name: 'Paraná' },
  { code: 'PE', name: 'Pernambuco' },
  { code: 'PI', name: 'Piauí' },
  { code: 'RJ', name: 'Rio de Janeiro' },
  { code: 'RN', name: 'Rio Grande do Norte' },
  { code: 'RS', name: 'Rio Grande do Sul' },
  { code: 'RO', name: 'Rondônia' },
  { code: 'RR', name: 'Roraima' },
  { code: 'SC', name: 'Santa Catarina' },
  { code: 'SP', name: 'São Paulo' },
  { code: 'SE', name: 'Sergipe' },
  { code: 'TO', name: 'Tocantins' },
];