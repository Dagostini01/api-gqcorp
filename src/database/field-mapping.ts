/**
 * Mapeamento de campos brutos dos JSONs dos robôs para o modelo normalizado
 * API GQCorp - Brasil, Peru, Chile
 */

// ===== INTERFACES PARA DADOS BRUTOS =====

export interface BrasilRawData {
  country_code: string;
  state: string;
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
    numerationDate: rawData.fecNumeracao,
    fobUsd: parseDecimal(rawData.fobUsd),
    freightUsd: parseDecimal(rawData.fleteUsd),
    insuranceUsd: parseDecimal(rawData.seguro),
    cifUsd: parseDecimal(rawData.cif),
    netWeight: parseDecimal(rawData.pesoNeto),
    operationDate: parseDate(rawData.ano_ref, rawData.mes_ref),
    dataSource: 'COMEXSTAT',
    rawData: rawData, // Preserva dados originais para auditoria
  };
}

/**
 * Converte dados brutos do Peru para o modelo normalizado
 */
export function transformPeruData(rawData: PeruRawData): Partial<ImportData> {
  return {
    declarationNumber: rawData.declaracao,
    fobUsd: parseDecimal(rawData.fobUsd),
    operationDate: parseDate(rawData.ano_ref, rawData.mes_ref),
    dataSource: 'ADUANET',
    rawData: rawData,
  };
}

/**
 * Converte dados brutos do Chile para o modelo normalizado
 */
export function transformChileData(rawData: ChileRawData): Partial<ImportData> {
  return {
    operationDate: parseDate(rawData.ano_ref, rawData.mes_ref),
    dataSource: 'CKAN_CHILE',
    rawData: rawData,
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
 * Converte string para Decimal
 */
function parseDecimal(value: string): number | undefined {
  if (!value || value.trim() === '') return undefined;
  const parsed = parseFloat(value.replace(',', '.'));
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