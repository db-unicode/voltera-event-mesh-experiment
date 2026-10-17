# 0001. Registro de decisiones de arquitectura

- Estado: aceptada
- Fecha: 2026-10-16

## Contexto y problema

El repo anterior (`payment-mesh-experiment`) documentó sus decisiones dentro de `architecture.md`, sin numeración ni estado. Cuesta citar qué se decidió, cuándo y qué alternativas se descartaron.

## Decisión

Cada decisión de arquitectura se registra como un ADR en `docs/adr/NNNN-titulo.md`, en formato MADR breve:

- Estado, fecha.
- Contexto y problema.
- Opciones consideradas.
- Decisión y justificación.
- Consecuencias, incluidos los límites declarados.

Los ADR son inmutables una vez aceptados. Un cambio se registra con un ADR nuevo que reemplaza al anterior y se enlaza desde él.

## Consecuencias

- Las decisiones se citan por número en el informe y en los commits.
- Cuesta un archivo corto por decisión; se acepta.
