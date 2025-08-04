# Configuración de Ascensos - README

Este directorio contiene la configuración externa para el sistema de ascensos policiales.

## Archivo: ascenso_config.json

Este archivo JSON contiene toda la configuración necesaria para el sistema de ascensos:

### Estructura del archivo:

```json
{
  "niveles_academicos": {
    "NIVEL": valor_numerico
  },
  "rangos": [
    "LISTA_DE_RANGOS_POLICIALES"
  ],
  "criterios_ascenso": {
    "RANGO_ACTUAL": {
      "siguienteRango": "RANGO_SIGUIENTE",
      "tiempoRango": años_requeridos_en_rango,
      "antiguedad": años_total_servicio_requerido,
      "nivelAcademico": "NIVEL_ACADEMICO_REQUERIDO"
    }
  }
}
```

### Descripción de campos:

#### niveles_academicos
Define la jerarquía académica con valores numéricos para comparación:
- Valores más altos = mayor nivel académico
- Se usa para verificar si un funcionario cumple el requisito académico

#### rangos
Lista ordenada de todos los rangos policiales disponibles en el sistema.

#### criterios_ascenso
Define los requisitos para ascender de cada rango:
- **siguienteRango**: El rango al que puede ascender
- **tiempoRango**: Años mínimos que debe tener en el rango actual
- **antiguedad**: Años mínimos de servicio total requeridos
- **nivelAcademico**: Nivel académico mínimo requerido

### Modificación del archivo:

1. **Editar directamente**: Puede modificar el archivo JSON con cualquier editor de texto
2. **Validar JSON**: Asegúrese de que el formato JSON sea válido
3. **Reiniciar servicio**: Después de modificar, reinicie el servicio ETL para aplicar cambios

### Ejemplo de modificación:

Para cambiar el tiempo requerido en rango para AGENTE de 2 a 3 años:

```json
"AGENTE": {
  "siguienteRango": "OFICIAL",
  "tiempoRango": 3,  // Cambiar de 2 a 3
  "antiguedad": 2,
  "nivelAcademico": "BACHILLER"
}
```

### Notas importantes:

- El sistema carga esta configuración al iniciar
- Si hay errores en el JSON, se usará la configuración por defecto
- Los cambios requieren reinicio del servicio para aplicarse
- Mantenga copias de respaldo antes de modificar

### Ubicación del archivo:
`etl_app/config/ascenso_config.json`

### Logs de errores:
Si hay problemas cargando la configuración, revise los logs en:
`etl_app/logs/etl_app.log`
