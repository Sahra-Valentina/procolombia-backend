import { Injectable, BadRequestException } from '@nestjs/common';
import * as xlsx from 'xlsx';
import { DatabaseService } from '../database/database.service';
import { toPostgresISO } from '../common/date.util';

@Injectable()
export class UploadService {
  constructor(private readonly databaseService: DatabaseService) {}

  async processFile(file: Express.Multer.File, type: string) {
    if (!file) {
      throw new BadRequestException('Archivo requerido');
    }


    switch (type) {
      case 'ANSENCUESTAS':
        return this.processANSENCUESTAS(file);

      case 'PRODUCTIVIDAD':
        return this.processPRODUCTIVIDAD(file);

      case 'BACKLOG':
        return this.processBACKLOG(file);

      case 'AVAYA':
        return this.processAVAYA(file);

      default:
        throw new BadRequestException('Tipo de archivo no soportado');
    }
  }

  // ANS ENCUESTAS
  async processANSENCUESTAS(file: Express.Multer.File) {
    const rows = this.parseExcel(file.buffer);

    console.log("TOTAL ROWS:", rows.length);

    const mapped = rows
      .map((row) => this.mapANSENCUESTAS(row))
      .filter((row) => row.numero_del_caso);

    console.log("TOTAL MAPPED:", mapped.length);

    const result = await this.databaseService.insertANSENCUESTAS(mapped);

    console.log("RESULTADO INSERT:", result);

    return result;
  }

  mapANSENCUESTAS(row: any) {
    return {
      numero_del_caso:
        row['__EMPTY'] ||
        row['Numero del caso'] ||
        row.numero_del_caso ||
        null,

      tipo_de_caso: row['Tipo de caso'] || null,

      fecha_creacion_encuesta: toPostgresISO(
        row['Fecha Creacion encuesta']
      ),

      fecha_resolucion_encuesta: toPostgresISO(
        row['Fecha resolución encuesta']
      ),

      cliente: row['Cliente'] || null,
      nombre_especialista: row['Especialista'] || null,

      como_calificas_en_tiempo_de_respuesta:
        row['¿Cómo calificas en tiempo de respuesta?'] || null,

      como_calificas_la_facilidad_en_el_contacto:
        row['¿Cómo calificas la facilidad en el contacto?'] || null,

      como_calificas_la_calidad_en_el_servicio:
        row['¿Cómo calificas la calidad en el servicio?'] || null,

      como_calificas_la_experiencia_y_el_conocimiento:
        row['¿Cómo calificas la experiencia y el conocimiento?'] || null,

      la_solución_brindada_cumplio_con_tu_necesidad_o_expectativa:
        row['¿La solución brindada cumplió con tu necesidad o expectativa?'] || null,

      justificacion: row['Justificación'] || null,
    };
  }

  //PRODUCTIVIDAD
  async processPRODUCTIVIDAD(file: Express.Multer.File) {
    const rows = this.parseExcel(file.buffer);

    const mapped = rows.map((row) => this.mapPRODUCTIVIDAD(row));

    console.log("TOTAL PRODUCTIVIDAD:", mapped.length);

    return this.databaseService.insertPRODUCTIVIDAD(mapped);
  }

mapPRODUCTIVIDAD(row: any) {
  return {
    proyecto: row['PROYECTO'] || null,
    tipo_de_caso: row['TIPO DE CASO'] || null,
    modelo: row['MODELO'] || null,
    numero_del_caso: row['NUMERO DEL CASO'] || null,
    jerarquia_categoria: row['JERARQUIA CATEGORIA'] || null,
    descripcion_del_caso: row['DESCRIPCION DEL CASO'] || null,
    nombre_autor: row['NOMBRE AUTOR'] || null,
    nombre_especialista: row['NOMBRE ESPECIALISTA'] || null,
    grupo_especialista: row['GRUPO ESPECIALISTA'] || null,
    estado_del_caso: row['ESTADO DEL CASO'] || null,
    estado_2: row['ESTADO 2'] || null,
    razon: row['RAZON'] || null,
    compania: row['COMPAÑIA'] || null,
    nombre_del_cliente: row['NOMBRE DEL CLIENTE'] || null,
    correo_cliente: row['CORREO CLIENTE'] || null,

    fecha_registro: toPostgresISO(row['FECHA REGISTRO']),
    tipo_de_registro: row['TIPO DE REGISTRO'] || null,
    fecha_de_cierre: toPostgresISO(row['FECHA DE CIERRE']),
    fecha_de_modificacion: toPostgresISO(row['FECHA DE MODIFICACION']),
    fecha_de_atencion_real: toPostgresISO(row['FECHA ATENCION REAL']),
    fecha_de_solucion_real: toPostgresISO(row['FECHA SOLUCION REAL']),

    impacto: row['IMPACTO'] || null,
    urgencia: row['URGENCIA'] || null,
    prioridad: row['PRIORIDAD'] || null,

    nombre_del_servicio: row['NOMBRE DEL SERVICIO'] || null,
    nombre_sla: row['NOMBRE SLA'] || null,

    fecha_de_solucion_estimada: toPostgresISO(row['FECHA SOLUCION ESTIMADA']),
    fecha_de_atencion_estimada: toPostgresISO(row['FECHA ATENCION ESTIMADA']),

    tiempo_del_caso: row['TIEMPO DEL CASO'] || null,
    progreso_del_caso: row['PROGRESO DEL CASO'] || null,
    tiempo_atencion_real: row['TIEMPO ATENCION REAL'] || null,
    tiempo_solucion_real: row['TIEMPO SOLUCION REAL'] || null,

    cumple_atencion: row['CUMPLE ATENCION'] || null,
    cumple_solucion: row['CUMPLE SOLUCION'] || null,

    comentario_solucion: row['COMENTARIO SOLUCION'] || null,
    asunto_del_caso: row['ASUNTO DEL CASO'] || null,

    fue_escalado: row['FUE ESCALADO'] || null,
    fecha_escalamiento: toPostgresISO(row['FECHA ESCALAMIENTO']),
    autor_escalamiento: row['AUTOR ESCALAMIENTO'] || null,

    cliente_departamento: row['CLIENTE DEPARTAMENTO'] || null,
    cliente_direccion: row['CLIENTE DIRECCION'] || null,
    cliente_ciudad: row['CLIENTE CIUDAD'] || null,
    cliente_pais: row['CLIENTE PAIS'] || null,

    ultima_nota: row['ULTIMA NOTA'] || null,
    fecha_nota: toPostgresISO(row['FECHA NOTA']),
  };
}

  //BACKLOG
  async processBACKLOG(file: Express.Multer.File) {
    const rows = this.parseExcel(file.buffer);

    const mapped = rows.map((row) => this.mapBACKLOG(row));

    console.log("TOTAL BACKLOG:", mapped.length);

    return this.databaseService.insertBACKLOG(mapped);
  }

mapBACKLOG(row: any) {
  return {
    caso_id: row['caso'] || null,
    responsable: row['nombre especialista'] || null,
    grupo_responsable: row['grupo especialista'] || null,
    estado_caso: row['estado del caso'] || null,
    cliente: row['nombre del cliente'] || null,
    fecha_registro: toPostgresISO(row['fecha registro']),
    prioridad: row['prioridad'] || null,
    servicio: row['nombre del servicio'] || null,
    progreso: row['progreso del caso'] || null,
    asunto: row['asunto del caso'] || null,
  };
}

  //AVAYA
async processAVAYA(file: Express.Multer.File) {
  const rows = this.parseExcel(file.buffer);

  const mapped = rows
    .map((row) => this.mapAVAYA(row))
    .filter((row) => {
      if (!row.fecha) {
        console.log("Fila sin fecha:", row);
      }
      return row.fecha;
    });

  console.log("TOTAL AVAYA:", mapped.length);

  return this.databaseService.insertAVAYA(mapped);
}

mapAVAYA(row: any) {
  return {
    fecha: toPostgresISO(row['fecha'], { onlyDate: true }),

    llamadas_ofrecidas: row['llamadas ofrecidas'] || 0,
    llamadas_respondidas: row['llamadas respondidas'] || 0,
    pct_llamadas_resp: row['% llamadas resp.'] || 0,
    vel_prom_resp: row['vel. prom. de resp.'] || 0,

    resp_0_5_sec: row['resp. 0 - 5 sec.'] || 0,
    resp_6_10_sec: row['resp. 6 - 10 sec.'] || 0,
    resp_11_20_sec: row['resp. 11 - 20 sec.'] || 0,
    resp_21_30_sec: row['resp. 21 - 30 sec.'] || 0,
    resp_31_35_sec: row['resp. 31 - 35 sec.'] || 0,
    resp_36_60_sec: row['resp. 36 - 60 sec.'] || 0,
    resp_61_120_sec: row['resp. 61 - 120 sec.'] || 0,
    resp_121_300_sec: row['resp. 121 - 300 sec.'] || 0,
    resp_301_600_sec: row['resp. 301 - 600 sec.'] || 0,
    resp_mayor_600_sec: row['resp. > 600 sec.'] || 0,

    pct_dentro_nivel_servicio: row['% dentro del nivel de servicio'] || 0,
    pct_fuera_nivel_servicio: row['% fuera del nivel de servicio'] || 0,

    llamadas_aban: row['llamadas aban.'] || 0,
    pct_llamadas_aban: row['% llamadas aban.'] || 0,
    tiempo_prom_aban: row['tiempo prom. de aban.'] || 0,

    abn_0_5_sec: row['abn. 0 - 5 sec.'] || 0,
    abn_6_10_sec: row['abn. 6 - 10 sec.'] || 0,
    abn_11_20_sec: row['abn. 11 - 20 sec.'] || 0,
    abn_21_30_sec: row['abn. 21 - 30 sec.'] || 0,
    abn_31_35_sec: row['abn. 31 - 35 sec.'] || 0,
    abn_36_60_sec: row['abn. 36 - 60 sec.'] || 0,
    abn_61_120_sec: row['abn.  61 - 120 sec.'] || 0,
    abn_121_300_sec: row['abn.  121 - 300 sec.'] || 0,
    abn_301_600_sec: row['abn.  301 - 600 sec.'] || 0,
    abn_mayor_600_sec: row['abn. > 600 sec.'] || 0,

    tiempo_prom_conversacion: row['tiempo prom. conversacion'] || 0
  };
}
  //UTIL
parseExcel(buffer: Buffer) {
  const workbook = xlsx.read(buffer, { type: 'buffer' });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];

  const raw = xlsx.utils.sheet_to_json(sheet, { defval: null });

  return raw.map((row: any) => {
    const newRow: any = {};

    for (const key in row) {
      const cleanKey = key.trim().toLowerCase();
      newRow[cleanKey] = row[key];
    }

    return newRow;
  });
}
}

