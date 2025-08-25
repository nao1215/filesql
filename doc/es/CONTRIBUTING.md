# Guía de Contribución

## Introducción

¡Gracias por considerar contribuir al proyecto filesql! Este documento explica cómo contribuir al proyecto. Damos la bienvenida a todas las formas de contribución, incluyendo contribuciones de código, mejoras de documentación, informes de errores y sugerencias de características.

## Configuración del Entorno de Desarrollo

### Requisitos Previos

#### Instalación de Go

El desarrollo de filesql requiere Go 1.24 o posterior.

**macOS (usando Homebrew)**
```bash
brew install go
```

**Linux (para Ubuntu)**
```bash
# Usando snap
sudo snap install go --classic

# O descargar desde el sitio oficial
wget https://go.dev/dl/go1.24.0.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.24.0.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.profile
source ~/.profile
```

**Windows**
Descarga y ejecuta el instalador desde el [sitio web oficial de Go](https://go.dev/dl/).

Verificar instalación:
```bash
go version
```

### Clonar el Proyecto

```bash
git clone https://github.com/nao1215/filesql.git
cd filesql
```

### Instalar Herramientas de Desarrollo

```bash
# Instalar las herramientas de desarrollo necesarias
make tools
```

### Verificación

Para verificar que tu entorno de desarrollo esté configurado correctamente, ejecuta los siguientes comandos:

```bash
# Ejecutar pruebas
make test

# Ejecutar linter
make lint
```

## Estructura del Proyecto

```
filesql/
├── domain/          # Capa del modelo de dominio
│   ├── model/      # Definiciones del modelo de dominio
│   └── repository/ # Interfaces del repositorio
├── driver/         # Implementación del controlador SQLite
├── testdata/       # Archivos de datos de prueba
├── doc/            # Documentación
│   ├── ja/        # Documentación en japonés
│   ├── zh-cn/     # Documentación en chino
│   ├── es/        # Documentación en español
│   └── ...        # Otros idiomas
├── filesql.go      # Punto de entrada principal de la biblioteca
├── filesql_test.go # Pruebas de la biblioteca
└── example_test.go # Ejemplos de uso
```

### Roles de los Directorios

- **domain/**: Capa que contiene la lógica de negocio y modelos de dominio. Implementación pura de Go sin dependencias externas
- **driver/**: Interfaz e implementación del controlador SQLite. Proporciona un controlador compatible con database/sql
- **testdata/**: Archivos de muestra como CSV, TSV, LTSV utilizados en las pruebas
- **doc/**: Documentación multiidioma con subdirectorios para cada idioma

## Flujo de Trabajo de Desarrollo

### Estrategia de Ramas

- La rama `main` es la última versión estable
- Crea nuevas ramas desde `main` para nuevas características o corrección de errores
- Ejemplos de nombres de ramas:
  - `feature/add-json-support` - Nueva característica
  - `fix/issue-123` - Corrección de error
  - `docs/update-readme` - Actualización de documentación

### Estándares de Codificación

Este proyecto sigue estos estándares:

1. **Conformarse con [Effective Go](https://go.dev/doc/effective_go)**
2. **Evitar el uso de variables globales** (excepto para el paquete config)
3. **Siempre agregar comentarios a funciones, variables y estructuras públicas**
4. **Mantener las funciones lo más pequeñas posible**
5. **Se recomienda escribir pruebas**

### Escribir Pruebas

Las pruebas son importantes. Por favor, sigue estas pautas:

1. **Pruebas unitarias**: Apuntar a una cobertura del 80% o superior
2. **Legibilidad de las pruebas**: Escribir casos de prueba claros
3. **Ejecución paralela**: Usar `t.Parallel()` siempre que sea posible

Ejemplo de prueba:
```go
func TestFile_Parse(t *testing.T) {
    t.Parallel()
    
    t.Run("should parse CSV file correctly", func(t *testing.T) {
        // Entrada y valores esperados claros para el caso de prueba
        input := "name,age\nAlice,30"
        expected := &Table{...}
        
        result, err := ParseCSV(input)
        assert.NoError(t, err)
        assert.Equal(t, expected, result)
    })
}
```

## Crear Pull Requests

### Preparación

1. **Verificar o Crear Issues**
   - Verifica si hay issues existentes
   - Para cambios importantes, recomendamos discutir el enfoque en un issue primero

2. **Escribir Pruebas**
   - Siempre agrega pruebas para nuevas características
   - Para correcciones de errores, crea pruebas que reproduzcan el error

3. **Verificación de Calidad**
   ```bash
   # Asegurar que todas las pruebas pasen
   make test
   
   # Verificación del linter
   make lint
   
   # Verificar cobertura (80% o superior)
   go test -cover ./...
   ```

### Enviar Pull Request

1. Crea un Pull Request desde tu repositorio bifurcado al repositorio principal
2. El título del PR debe describir brevemente los cambios
3. Incluye lo siguiente en la descripción del PR:
   - Propósito y contenido de los cambios
   - Número de issue relacionado (si existe)
   - Método de prueba
   - Pasos de reproducción para correcciones de errores

### Acerca de CI/CD

GitHub Actions verifica automáticamente los siguientes elementos:

- **Pruebas multiplataforma**: Ejecución de pruebas en Linux, macOS y Windows
- **Verificación del linter**: Análisis estático con golangci-lint
- **Cobertura de pruebas**: Mantener una cobertura del 80% o superior
- **Verificación de compilación**: Compilaciones exitosas en cada plataforma

No es posible fusionar a menos que pasen todas las verificaciones.

## Informes de Errores

Cuando encuentres un error, por favor crea un issue con la siguiente información:

1. **Información del Entorno**
   - SO (Linux/macOS/Windows) y versión
   - Versión de Go
   - Versión de filesql

2. **Pasos de Reproducción**
   - Ejemplo de código mínimo para reproducir el error
   - Archivos de datos utilizados (si es posible)

3. **Comportamiento Esperado y Real**

4. **Mensajes de Error o Trazas de Pila** (si los hay)

## Contribuir Fuera del Código

Las siguientes actividades también son muy bienvenidas:

### Actividades que Aumentan la Motivación

- **Dar una Estrella en GitHub**: Muestra tu interés en el proyecto
- **Promover el Proyecto**: Preséntalo en blogs, redes sociales, grupos de estudio, etc.
- **Convertirse en Patrocinador de GitHub**: Apoyo disponible en [https://github.com/sponsors/nao1215](https://github.com/sponsors/nao1215)

### Otras Formas de Contribuir

- **Mejoras de Documentación**: Corregir errores tipográficos, mejorar la claridad de las explicaciones
- **Traducciones**: Traducir la documentación a nuevos idiomas
- **Agregar Ejemplos**: Proporcionar código de muestra práctico
- **Sugerencias de Características**: Compartir ideas de nuevas características en issues

## Comunidad

### Código de Conducta

Por favor, consulta [CODE_OF_CONDUCT.md](../../CODE_OF_CONDUCT.md). Esperamos que todos los contribuyentes se traten con respeto.

### Preguntas e Informes

- **GitHub Issues**: Informes de errores y sugerencias de características

## Licencia

Las contribuciones a este proyecto se consideran publicadas bajo la licencia del proyecto (Licencia MIT).

---

¡Gracias nuevamente por considerar contribuir! Esperamos sinceramente tu participación.