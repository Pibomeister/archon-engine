import { useEffect, useCallback } from 'react';

export interface BuilderKeyboardActions {
  onSave: () => void;
  onUndo: () => void;
  onRedo: () => void;
  onToggleLibrary: () => void;
  onToggleYaml: () => void;
  onToggleValidation: () => void;
  onAddPrompt: () => void;
  onAddBash: () => void;
  onDeleteSelected: () => void;
  onDuplicateSelected: () => void;
  onQuickAdd?: () => void;
  onFitView?: () => void;
  onSelectAll?: () => void;
}

const EDITABLE_ARIA_ROLES = new Set(['combobox', 'textbox', 'searchbox']);

export function isInputTarget(e: KeyboardEvent): boolean {
  const target = e.target as HTMLElement | null;
  if (!target) return false;
  const tag = target.tagName;
  if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return true;
  if (target.isContentEditable) return true;
  const role = target.getAttribute?.('role');
  if (role && EDITABLE_ARIA_ROLES.has(role)) return true;
  return false;
}

type KeyboardAction = (actions: BuilderKeyboardActions) => void;

function runPreventedAction(
  e: KeyboardEvent,
  actions: BuilderKeyboardActions,
  action: KeyboardAction
): void {
  e.preventDefault();
  action(actions);
}

function alwaysActiveShortcut(e: KeyboardEvent): KeyboardAction | null {
  if (!(e.metaKey || e.ctrlKey)) return null;
  if (e.key === 's')
    return actions => {
      actions.onSave();
    };
  if (e.key === 'z' && e.shiftKey)
    return actions => {
      actions.onRedo();
    };
  if (e.key === 'z')
    return actions => {
      actions.onUndo();
    };
  if (e.key === '\\')
    return actions => {
      actions.onToggleLibrary();
    };
  if (e.key === 'j')
    return actions => {
      actions.onToggleYaml();
    };
  if (e.key === '.')
    return actions => {
      actions.onToggleValidation();
    };
  return null;
}

function modifierCanvasShortcut(e: KeyboardEvent): KeyboardAction | null {
  if (!(e.metaKey || e.ctrlKey)) return null;
  if (e.key === 'd')
    return actions => {
      actions.onDuplicateSelected();
    };
  if (e.key === '0') return actions => actions.onFitView?.();
  if (e.key === 'a') return actions => actions.onSelectAll?.();
  return null;
}

function singleKeyCanvasShortcut(e: KeyboardEvent): KeyboardAction | null {
  if (e.key === 'n') return actions => actions.onQuickAdd?.();
  if (e.key === 'p')
    return actions => {
      actions.onAddPrompt();
    };
  if (e.key === 'b')
    return actions => {
      actions.onAddBash();
    };
  if (e.key === 'f') return actions => actions.onFitView?.();
  if (e.key === 'Delete' || e.key === 'Backspace')
    return actions => {
      actions.onDeleteSelected();
    };
  return null;
}

export function handleBuilderKeydown(
  e: KeyboardEvent,
  actions: BuilderKeyboardActions,
  enabled = true
): void {
  if (!enabled) return;

  const globalAction = alwaysActiveShortcut(e);
  if (globalAction !== null) {
    runPreventedAction(e, actions, globalAction);
    return;
  }

  if (isInputTarget(e)) return;

  const modifierAction = modifierCanvasShortcut(e);
  if (modifierAction !== null) {
    runPreventedAction(e, actions, modifierAction);
    return;
  }

  const singleKeyAction = singleKeyCanvasShortcut(e);
  if (singleKeyAction === null) return;

  if (e.key === 'Delete' || e.key === 'Backspace') {
    e.preventDefault();
  }
  singleKeyAction(actions);
}

export function useBuilderKeyboard(actions: BuilderKeyboardActions, enabled = true): void {
  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      handleBuilderKeydown(e, actions, enabled);
    },
    [actions, enabled]
  );

  useEffect(() => {
    window.addEventListener('keydown', handleKeyDown);
    return (): void => {
      window.removeEventListener('keydown', handleKeyDown);
    };
  }, [handleKeyDown]);
}
