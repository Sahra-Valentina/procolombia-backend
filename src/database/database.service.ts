import { Injectable, InternalServerErrorException } from '@nestjs/common';
import { Pool, PoolClient, QueryResult } from 'pg';

@Injectable()
export class DatabaseService {
  private pool: Pool;

  constructor() {
    this.pool = new Pool({
      host: process.env.DB_HOST,
      port: Number(process.env.DB_PORT),
      user: process.env.DB_USER,
      password: process.env.DB_PASS,
      database: process.env.DB_NAME,
      ssl: { rejectUnauthorized: false },
    });
  }

  //  GENERADOR DE LOGS
  private logStart(name: string, rows: any[]) {
    console.log(`\n🚀 [${name}] Inicio carga`);
    console.log(`📥 Registros recibidos: ${rows?.length || 0}`);
  }

  private logEnd(name: string, stats: any, startTime: number) {
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);

    console.log(`\n✅ [${name}] Carga finalizada`);
    console.log(`⏱️ Tiempo: ${duration}s`);
    console.log(`📊 Total: ${stats.total}`);
    console.log(`🟢 Insertados: ${stats.inserted}`);
    console.log(`🟡 Duplicados: ${stats.duplicates}`);
    console.log(`🔴 Errores: ${stats.errors}`);
    console.log(`⚠️ Inválidos: ${stats.invalid}`);

    return { ...stats, duration: `${duration}s` };
  }

// ENCUESTAS
async insertANSENCUESTAS(rows: any[]) {
  const startTime = Date.now();
  this.logStart('ANS', rows);

  if (!rows?.length) {
    return { message: 'No hay datos', total: 0 };
  }

  let client: PoolClient | null = null;
  let inserted = 0, errors = 0, duplicates = 0, invalid = 0;

  try {
    client = await this.pool.connect();
    await client.query('BEGIN');

    for (const row of rows) {

      if (!row.numero_del_caso) {
        invalid++;
        continue;
      }

      try {
        const result: QueryResult = await client.query(
          `
          INSERT INTO procolombia_encuestas_ans (
            numero_del_caso,
            tipo_de_caso,
            fecha_creacion_encuesta,
            fecha_resolucion_encuesta,
            cliente,
            nombre_especialista,
            como_calificas_en_tiempo_de_respuesta,
            como_calificas_la_facilidad_en_el_contacto,
            como_calificas_la_calidad_en_el_servicio,
            como_calificas_la_experiencia_y_el_conocimiento,
            la_solución_brindada_cumplio_con_tu_necesidad_o_expectativa,
            justificacion
          )
          VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12
          )
          ON CONFLICT (numero_del_caso) DO UPDATE SET
            tipo_de_caso = EXCLUDED.tipo_de_caso,
            fecha_creacion_encuesta = EXCLUDED.fecha_creacion_encuesta,
            fecha_resolucion_encuesta = EXCLUDED.fecha_resolucion_encuesta,
            cliente = EXCLUDED.cliente,
            nombre_especialista = EXCLUDED.nombre_especialista,
            como_calificas_en_tiempo_de_respuesta = EXCLUDED.como_calificas_en_tiempo_de_respuesta,
            como_calificas_la_facilidad_en_el_contacto = EXCLUDED.como_calificas_la_facilidad_en_el_contacto,
            como_calificas_la_calidad_en_el_servicio = EXCLUDED.como_calificas_la_calidad_en_el_servicio,
            como_calificas_la_experiencia_y_el_conocimiento = EXCLUDED.como_calificas_la_experiencia_y_el_conocimiento,
            la_solución_brindada_cumplio_con_tu_necesidad_o_expectativa = EXCLUDED.la_solución_brindada_cumplio_con_tu_necesidad_o_expectativa,
            justificacion = EXCLUDED.justificacion
          WHERE
            procolombia_encuestas_ans.fecha_creacion_encuesta IS DISTINCT FROM EXCLUDED.fecha_creacion_encuesta
            OR procolombia_encuestas_ans.fecha_resolucion_encuesta IS DISTINCT FROM EXCLUDED.fecha_resolucion_encuesta
            OR procolombia_encuestas_ans.tipo_de_caso IS DISTINCT FROM EXCLUDED.tipo_de_caso
            OR procolombia_encuestas_ans.cliente IS DISTINCT FROM EXCLUDED.cliente
            OR procolombia_encuestas_ans.justificacion IS DISTINCT FROM EXCLUDED.justificacion
          `,
          [
            row.numero_del_caso ?? null,
            row.tipo_de_caso ?? null,
            row.fecha_creacion_encuesta ?? null,
            row.fecha_resolucion_encuesta ?? null,
            row.cliente ?? null,
            row.nombre_especialista ?? null,
            row.como_calificas_en_tiempo_de_respuesta ?? null,
            row.como_calificas_la_facilidad_en_el_contacto ?? null,
            row.como_calificas_la_calidad_en_el_servicio ?? null,
            row.como_calificas_la_experiencia_y_el_conocimiento ?? null,
            row.la_solución_brindada_cumplio_con_tu_necesidad_o_expectativa ?? null,
            row.justificacion ?? null
          ],
        );

        if (result.rowCount === 1) inserted++;
        else duplicates++;

      } catch (e) {
        errors++;
        console.error(`❌ ANS ${row.numero_del_caso}`, e);
      }
    }

    await client.query('COMMIT');

    return this.logEnd('ANS', {
      total: rows.length,
      inserted,
      duplicates,
      errors,
      invalid
    }, startTime);

  } catch (error) {
    await client?.query('ROLLBACK');
    console.error('💥 Error ANS', error);
    throw new InternalServerErrorException('Error ANS');
  } finally {
    client?.release();
  }
}

// PRODUCTIVIDAD
async insertPRODUCTIVIDAD(rows: any[]) {
  const startTime = Date.now();
  this.logStart('PRODUCTIVIDAD', rows);

  if (!rows?.length) return { message: 'No hay datos' };

  let client: PoolClient | null = null;
  let inserted = 0, errors = 0, duplicates = 0, invalid = 0;

  try {
    client = await this.pool.connect();
    await client.query('BEGIN');

    for (const row of rows) {

      if (!row.numero_del_caso) {
        invalid++;
        continue;
      }

      try {
        const result = await client.query(
          `
          INSERT INTO procolombia_productividad (
            proyecto,
            tipo_de_caso,
            modelo,
            numero_del_caso,
            jerarquia_categoria,
            nombre_autor,
            nombre_especialista,
            grupo_especialista,
            estado_del_caso,
            estado_2,
            razon,
            compania,
            nombre_del_cliente,
            correo_cliente,
            fecha_registro,
            tipo_de_registro,
            fecha_de_cierre,
            fecha_de_modificacion,
            fecha_de_atencion_real,
            fecha_de_solucion_real,
            impacto,
            urgencia,
            prioridad,
            nombre_del_servicio,
            nombre_sla,
            fecha_de_solucion_estimada,
            fecha_de_atencion_estimada,
            tiempo_del_caso,
            progreso_del_caso,
            tiempo_atencion_real,
            tiempo_solucion_real,
            cumple_atencion,
            cumple_solucion,
            asunto_del_caso,
            fue_escalado,
            fecha_escalamiento,
            autor_escalamiento,
            cliente_departamento
          )
          VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
            $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
            $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,
            $31,$32,$33,$34,$35,$36,$37,$38
          )
          ON CONFLICT (numero_del_caso) DO UPDATE SET
            estado_del_caso = EXCLUDED.estado_del_caso,
            estado_2 = EXCLUDED.estado_2,
            prioridad = EXCLUDED.prioridad,
            nombre_especialista = EXCLUDED.nombre_especialista,
            grupo_especialista = EXCLUDED.grupo_especialista,
            fecha_de_modificacion = EXCLUDED.fecha_de_modificacion,
            fecha_de_cierre = EXCLUDED.fecha_de_cierre,
            progreso_del_caso = EXCLUDED.progreso_del_caso,
            fecha_registro = EXCLUDED.fecha_registro
          WHERE
            procolombia_productividad.estado_del_caso IS DISTINCT FROM EXCLUDED.estado_del_caso
            OR procolombia_productividad.prioridad IS DISTINCT FROM EXCLUDED.prioridad
            OR procolombia_productividad.progreso_del_caso IS DISTINCT FROM EXCLUDED.progreso_del_caso
            OR procolombia_productividad.fecha_de_modificacion IS DISTINCT FROM EXCLUDED.fecha_de_modificacion
            OR procolombia_productividad.fecha_registro IS DISTINCT FROM EXCLUDED.fecha_registro
            OR procolombia_productividad.fecha_de_cierre IS DISTINCT FROM EXCLUDED.fecha_de_cierre
          `,
          [
            row.proyecto,
            row.tipo_de_caso,
            row.modelo,
            row.numero_del_caso,
            row.jerarquia_categoria,
            row.nombre_autor,
            row.nombre_especialista,
            row.grupo_especialista,
            row.estado_del_caso,
            row.estado_2,
            row.razon,
            row.compania,
            row.nombre_del_cliente,
            row.correo_cliente,
            row.fecha_registro,
            row.tipo_de_registro,
            row.fecha_de_cierre,
            row.fecha_de_modificacion,
            row.fecha_de_atencion_real,
            row.fecha_de_solucion_real,
            row.impacto,
            row.urgencia,
            row.prioridad,
            row.nombre_del_servicio,
            row.nombre_sla,
            row.fecha_de_solucion_estimada,
            row.fecha_de_atencion_estimada,
            row.tiempo_del_caso ?? null,
            row.progreso_del_caso ?? null,
            row.tiempo_atencion_real ?? null,
            row.tiempo_solucion_real ?? null,
            row.cumple_atencion,
            row.cumple_solucion,
            row.asunto_del_caso,
            row.fue_escalado,
            row.fecha_escalamiento,
            row.autor_escalamiento,
            row.cliente_departamento
          ]
        );

        if (result.rowCount === 1) inserted++;
        else duplicates++;

      } catch (e) {
        errors++;
        console.error(`❌ PRODUCTIVIDAD ${row.numero_del_caso}`, e);
      }
    }

    await client.query('COMMIT');

    return this.logEnd('PRODUCTIVIDAD', {
      total: rows.length,
      inserted,
      duplicates,
      errors,
      invalid
    }, startTime);

  } catch (error) {
    await client?.query('ROLLBACK');
    throw new InternalServerErrorException('Error PRODUCTIVIDAD');
  } finally {
    client?.release();
  }
}

// BACKLOG
async insertBACKLOG(rows: any[]) {
  const startTime = Date.now();
  this.logStart('BACKLOG', rows);

  if (!rows?.length) return { message: 'No hay datos' };

  let client: PoolClient | null = null;
  let inserted = 0, updated = 0, errors = 0, invalid = 0;

  try {
    client = await this.pool.connect();
    await client.query('BEGIN');

    for (const row of rows) {

      if (!row.caso_id) {
        invalid++;
        continue;
      }

      try {
        const result = await client.query(
          `
          INSERT INTO procolombia_backlog_bt (
            caso_id,
            responsable,
            grupo_responsable,
            estado_caso,
            cliente,
            fecha_registro,
            prioridad,
            servicio,
            progreso,
            asunto
          )
          VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10
          )
          ON CONFLICT (caso_id) DO UPDATE SET
            responsable = EXCLUDED.responsable,
            grupo_responsable = EXCLUDED.grupo_responsable,
            estado_caso = EXCLUDED.estado_caso,
            cliente = EXCLUDED.cliente,
            fecha_registro = EXCLUDED.fecha_registro,
            prioridad = EXCLUDED.prioridad,
            servicio = EXCLUDED.servicio,
            progreso = EXCLUDED.progreso,
            asunto = EXCLUDED.asunto
          WHERE
            procolombia_backlog_bt.estado_caso IS DISTINCT FROM EXCLUDED.estado_caso
            OR procolombia_backlog_bt.progreso IS DISTINCT FROM EXCLUDED.progreso
            OR procolombia_backlog_bt.fecha_registro IS DISTINCT FROM EXCLUDED.fecha_registro
            OR procolombia_backlog_bt.prioridad IS DISTINCT FROM EXCLUDED.prioridad
            OR procolombia_backlog_bt.responsable IS DISTINCT FROM EXCLUDED.responsable
          RETURNING xmax
          `,
          [
            row.caso_id,
            row.responsable,
            row.grupo_responsable,
            row.estado_caso,
            row.cliente,
            row.fecha_registro,
            row.prioridad,
            row.servicio,
            row.progreso ?? null,
            row.asunto
          ],
        );

        if (result.rowCount === 1) {
          // Detectar si fue insert o update
          const isUpdate = result.rows[0]?.xmax !== '0';
          isUpdate ? updated++ : inserted++;
        }

      } catch (e) {
        errors++;
        console.error(`❌ BACKLOG ${row.caso_id}`, e);
      }
    }

    await client.query('COMMIT');

    console.log(`🔄 Actualizados: ${updated}`);

    return this.logEnd('BACKLOG', {
      total: rows.length,
      inserted,
      updated,
      duplicates: rows.length - inserted - updated - errors - invalid,
      errors,
      invalid
    }, startTime);

  } catch (error) {
    await client?.query('ROLLBACK');
    throw new InternalServerErrorException('Error BACKLOG');
  } finally {
    client?.release();
  }
}


// AVAYA
async insertAVAYA(rows: any[]) {
  const startTime = Date.now();
  this.logStart('AVAYA', rows);

  if (!rows?.length) return { message: 'No hay datos' };

  let client: PoolClient | null = null;
  let inserted = 0, errors = 0, duplicates = 0, invalid = 0;

  try {
    client = await this.pool.connect();
    await client.query('BEGIN');

    for (const row of rows) {

      if (!row.fecha) {
        invalid++;
        continue;
      }

      try {
        const result = await client.query(
          `
          INSERT INTO ans_data_avaya_procolombia (
            fecha,
            llamadas_ofrecidas,
            llamadas_respondidas,
            pct_llamadas_resp,
            vel_prom_resp,
            resp_0_5_sec,
            resp_6_10_sec,
            resp_11_20_sec,
            resp_21_30_sec,
            resp_31_35_sec,
            resp_36_60_sec,
            resp_61_120_sec,
            resp_121_300_sec,
            resp_301_600_sec,
            resp_mayor_600_sec,
            pct_dentro_nivel_servicio,
            pct_fuera_nivel_servicio,
            llamadas_aban,
            pct_llamadas_aban,
            tiempo_prom_aban,
            abn_0_5_sec,
            abn_6_10_sec,
            abn_11_20_sec,
            abn_21_30_sec,
            abn_31_35_sec,
            abn_36_60_sec,
            abn_61_120_sec,
            abn_121_300_sec,
            abn_301_600_sec,
            abn_mayor_600_sec,
            tiempo_prom_conversacion
          )
          VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
            $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
            $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31
          )
          ON CONFLICT (fecha) DO UPDATE SET
            llamadas_ofrecidas = EXCLUDED.llamadas_ofrecidas,
            llamadas_respondidas = EXCLUDED.llamadas_respondidas,
            pct_llamadas_resp = EXCLUDED.pct_llamadas_resp,
            vel_prom_resp = EXCLUDED.vel_prom_resp,
            resp_0_5_sec = EXCLUDED.resp_0_5_sec,
            resp_6_10_sec = EXCLUDED.resp_6_10_sec,
            resp_11_20_sec = EXCLUDED.resp_11_20_sec,
            resp_21_30_sec = EXCLUDED.resp_21_30_sec,
            resp_31_35_sec = EXCLUDED.resp_31_35_sec,
            resp_36_60_sec = EXCLUDED.resp_36_60_sec,
            resp_61_120_sec = EXCLUDED.resp_61_120_sec,
            resp_121_300_sec = EXCLUDED.resp_121_300_sec,
            resp_301_600_sec = EXCLUDED.resp_301_600_sec,
            resp_mayor_600_sec = EXCLUDED.resp_mayor_600_sec,
            pct_dentro_nivel_servicio = EXCLUDED.pct_dentro_nivel_servicio,
            pct_fuera_nivel_servicio = EXCLUDED.pct_fuera_nivel_servicio,
            llamadas_aban = EXCLUDED.llamadas_aban,
            pct_llamadas_aban = EXCLUDED.pct_llamadas_aban,
            tiempo_prom_aban = EXCLUDED.tiempo_prom_aban,
            abn_0_5_sec = EXCLUDED.abn_0_5_sec,
            abn_6_10_sec = EXCLUDED.abn_6_10_sec,
            abn_11_20_sec = EXCLUDED.abn_11_20_sec,
            abn_21_30_sec = EXCLUDED.abn_21_30_sec,
            abn_31_35_sec = EXCLUDED.abn_31_35_sec,
            abn_36_60_sec = EXCLUDED.abn_36_60_sec,
            abn_61_120_sec = EXCLUDED.abn_61_120_sec,
            abn_121_300_sec = EXCLUDED.abn_121_300_sec,
            abn_301_600_sec = EXCLUDED.abn_301_600_sec,
            abn_mayor_600_sec = EXCLUDED.abn_mayor_600_sec,
            tiempo_prom_conversacion = EXCLUDED.tiempo_prom_conversacion
          WHERE
            ans_data_avaya_procolombia.llamadas_respondidas IS DISTINCT FROM EXCLUDED.llamadas_respondidas
            OR ans_data_avaya_procolombia.pct_dentro_nivel_servicio IS DISTINCT FROM EXCLUDED.pct_dentro_nivel_servicio
            OR ans_data_avaya_procolombia.resp_0_5_sec IS DISTINCT FROM EXCLUDED.resp_0_5_sec
          `,
          [
            row.fecha ?? null,
            row.llamadas_ofrecidas ?? null,
            row.llamadas_respondidas ?? null,
            row.pct_llamadas_resp ?? null,
            row.vel_prom_resp ?? null,
            row.resp_0_5_sec ?? null,
            row.resp_6_10_sec ?? null,
            row.resp_11_20_sec ?? null,
            row.resp_21_30_sec ?? null,
            row.resp_31_35_sec ?? null,
            row.resp_36_60_sec ?? null,
            row.resp_61_120_sec ?? null,
            row.resp_121_300_sec ?? null,
            row.resp_301_600_sec ?? null,
            row.resp_mayor_600_sec ?? null,
            row.pct_dentro_nivel_servicio ?? null,
            row.pct_fuera_nivel_servicio ?? null,
            row.llamadas_aban ?? null,
            row.pct_llamadas_aban ?? null,
            row.tiempo_prom_aban ?? null,
            row.abn_0_5_sec ?? null,
            row.abn_6_10_sec ?? null,
            row.abn_11_20_sec ?? null,
            row.abn_21_30_sec ?? null,
            row.abn_31_35_sec ?? null,
            row.abn_36_60_sec ?? null,
            row.abn_61_120_sec ?? null,
            row.abn_121_300_sec ?? null,
            row.abn_301_600_sec ?? null,
            row.abn_mayor_600_sec ?? null,
            row.tiempo_prom_conversacion ?? null
          ]
        );

        if (result.rowCount === 1) inserted++;
        else duplicates++;

      } catch (e) {
        errors++;
        console.error(`❌ AVAYA ${row.fecha}`, e);
      }
    }

    await client.query('COMMIT');

    return this.logEnd('AVAYA', {
      total: rows.length,
      inserted,
      duplicates,
      errors,
      invalid
    }, startTime);

  } catch (error) {
    await client?.query('ROLLBACK');
    console.error('💥 Error AVAYA', error);
    throw new InternalServerErrorException('Error AVAYA');
  } finally {
    client?.release();
  }
}
}